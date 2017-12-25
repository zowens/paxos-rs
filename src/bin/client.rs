extern crate tokio_core;
extern crate paxos;
extern crate clap;
extern crate futures;
extern crate env_logger;

use std::net::SocketAddr;
use clap::{App, Arg, SubCommand};
use futures::{Future, Stream, Sink};
use paxos::UdpClient;
use paxos::messages::ClientMessage;
use tokio_core::reactor::Core;

fn main() {
    env_logger::init().unwrap();

    let matches = App::new("paxos client")
        .version("0.1")
        .arg(Arg::with_name("node")
             .short("n")
             .global(true)
             .default_value("0")
             .possible_values(&["0", "1", "2"])
             .help("Server node id"))
        .subcommand(SubCommand::with_name("propose")
                    .about("proposes a value")
                    .arg(Arg::with_name("VALUE")
                         .required(true)
                         .index(1)))
        .subcommand(SubCommand::with_name("get")
                    .about("gets the current value"))
        .get_matches();

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let client = UdpClient::new(&addr, &handle).unwrap();
    let (sink, stream) = client.split();

    let server_addr: SocketAddr = format!("127.0.0.1:400{}", matches.value_of("node").unwrap_or("0")).parse().unwrap();

    match matches.subcommand() {
        ("propose", Some(args)) => {
            let value = args.value_of("VALUE").unwrap();
            let msg = ClientMessage::ProposeRequest(server_addr, value.as_bytes().to_vec());
            core.run(sink.send(msg).map(|_| ()).map_err(|_| ())).unwrap();
        },
        ("get", Some(args)) => {
            let msg = ClientMessage::LookupValueRequest(server_addr);
            core.run(sink.send(msg).and_then(|_| {
                stream.take(1).for_each(move |msg| {
                    println!("recv {:?}", msg);
                    Ok(())
                })
            }).map_err(|_| ())).unwrap();
        }
        _ => {
            panic!("UNKNOWN COMMAND")
        }
    };


}
