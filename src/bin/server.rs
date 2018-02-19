#![feature(ip_constructors)]
extern crate env_logger;
extern crate futures;
extern crate paxos;
extern crate tokio_core;
extern crate tokio_io;

use std::borrow::Borrow;
use std::env::args;
use std::net::SocketAddr;
use std::net::Ipv4Addr;
use std::str;
use futures::{Future, Stream};
use tokio_core::net::TcpListener;
use tokio_core::reactor::Handle;
use tokio_io::AsyncRead;
use tokio_io::codec::LinesCodec;
use paxos::{BytesValue, Configuration, MultiPaxosBuilder, ProposalSender, Register,
            ReplicatedState, UdpServer};

fn local_config(node: u16) -> (Configuration, SocketAddr) {
    assert!(node < 3);

    let ip = Ipv4Addr::localhost().into();
    let current = (node as u32, SocketAddr::new(ip, 3000 + node));
    let client_addr = SocketAddr::new(ip, 4000 + node);
    let others = (0..3u16)
        .filter(|n| *n != node)
        .map(|n| (n as u32, SocketAddr::new(ip, 3000 + n)));
    (Configuration::new(current, others), client_addr)
}

enum Command {
    Get,
    Propose,
}

/// Allow clients to send messages to issue commands to the node:
///
/// Examples:
/// * `get`
/// * `propose hello world`
fn spawn_client_handler(
    register: Register,
    addr: SocketAddr,
    handle: Handle,
    proposals: ProposalSender<BytesValue>,
) {
    let socket = TcpListener::bind(&addr, &handle).unwrap();

    let handle_inner = handle.clone();
    let server = socket.incoming().for_each(move |(socket, _)| {
        let register = register.clone();
        let proposals = proposals.clone();

        let (sink, stream) = socket.framed(LinesCodec::new()).split();
        let client_future = stream
            .map(move |mut req| {
                let proposals = proposals.clone();
                let cmd = {
                    req.split_whitespace().next().and_then(|cmd| match cmd {
                        "get" => Some(Command::Get),
                        "propose" => Some(Command::Propose),
                        _ => None,
                    })
                };

                match cmd {
                    Some(Command::Get) => match register.snapshot(0) {
                        Some(v) => {
                            let v: &[u8] = v.borrow();
                            str::from_utf8(v)
                                .map(|v| v.to_string())
                                .unwrap_or_else(|_| "ERR: Value not UTF-8".to_string())
                        }
                        None => "ERR: No Value".to_string(),
                    },
                    Some(Command::Propose) => {
                        let value = req.split_off(8).into();
                        proposals
                            .propose(value)
                            .map(|_| "OK".to_string())
                            .unwrap_or_else(|_| "ERR: Unable to propose".to_string())
                    }
                    None => "ERR: Invalid command".to_string(),
                }
            })
            .forward(sink);
        handle_inner.spawn(client_future.map(|_| ()).map_err(|_| ()));

        Ok(())
    });

    handle.spawn(server.map_err(|_| ()));
}

pub fn main() {
    env_logger::init().unwrap();

    let (config, client_addr) = match args().nth(1) {
        Some(v) => local_config(v.parse::<u16>().unwrap()),
        None => panic!("Must specific node ID (0, 1, 2) in first argument"),
    };

    println!("{:?}", config);

    let register = Register::default();
    let server = UdpServer::new(config.clone()).unwrap();

    let (proposal_sink, multi_paxos) = MultiPaxosBuilder::new(config).build();
    spawn_client_handler(register, client_addr, server.handle(), proposal_sink);
    server.run(multi_paxos).unwrap();
}
