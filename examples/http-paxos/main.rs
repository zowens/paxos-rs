extern crate bincode;
extern crate bytes;
extern crate env_logger;
extern crate paxos;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate log;
extern crate hyper;
#[cfg(not(test))]
extern crate tokio;

mod commands;
mod kvstore;
mod service;

use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};
use paxos::{Configuration, Liveness, NodeId, Replica};
use std::{env::args, net::SocketAddr, process::exit};

fn config() -> paxos::Configuration {
    let node_id_str = match args().skip(1).next() {
        Some(node_id_str) => node_id_str,
        None => {
            error!("Must supply node ID as the first argument (0, 1, 2)");
            exit(1);
        }
    };

    let node_id = match u32::from_str_radix(&node_id_str, 10) {
        Ok(node_id) => node_id,
        Err(_) => {
            error!("Must supply integer node ID as the first argument (0, 1, 2)");
            exit(1);
        }
    };

    if node_id > 2 {
        error!("Must supply integer node ID as the first argument (0, 1, 2)");
        exit(1);
    }

    Configuration::new(
        node_id,
        (0u32..3)
            .filter(|n| *n != node_id)
            .map(|n| (n as NodeId, format!("127.0.0.1:808{}", n).parse().unwrap())),
    )
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let conf = config();
    let addr: SocketAddr = format!("127.0.0.1:808{}", conf.current()).parse().unwrap();
    let sender = commands::PaxosSender::new(&conf);
    let handler = service::Handler::new(Liveness::new(Replica::new(sender, conf)));
    let cleanup = handler.spawn_cleanup_loop();
    let tick = handler.spawn_tick_loop();

    let service = make_service_fn(move |_| {
        let handler = handler.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let handler = handler.clone();
                async move { handler.handle(req).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);
    if let Err(e) = server.await {
        drop(tick);
        drop(cleanup);
        eprintln!("server error: {}", e);
    }
}
