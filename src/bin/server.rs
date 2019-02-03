extern crate env_logger;
extern crate futures;
extern crate paxos;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;

use futures::{Future, Stream};
use paxos::{
    BytesValue, Configuration, MultiPaxosBuilder, ProposalSender, Register, ReplicatedState,
    UdpServer,
};
use std::borrow::Borrow;
use std::env::args;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::str;
use tokio::net::TcpListener;
use tokio_codec::{Decoder, LinesCodec};

fn local_config(node: u16) -> (Configuration, SocketAddr) {
    assert!(node < 3);

    let ip = Ipv4Addr::LOCALHOST.into();
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
fn client_handler(
    register: Register,
    addr: SocketAddr,
    proposals: ProposalSender<BytesValue>,
) -> impl Future<Item = (), Error = ()> {
    let socket = TcpListener::bind(&addr).unwrap();

    let server = socket.incoming().for_each(move |socket| {
        let register = register.clone();
        let proposals = proposals.clone();

        let (sink, stream) = LinesCodec::new().framed(socket).split();
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
        client_future.map(|_| ())
    });

    server.map_err(|_| ())
}

pub fn main() {
    env_logger::init().unwrap();

    let (config, client_addr) = match args().nth(1) {
        Some(v) => local_config(v.parse::<u16>().unwrap()),
        None => panic!("Must specific node ID (0, 1, 2) in first argument"),
    };

    println!("{:?}", config);

    let register = Register::new();
    let mut server = UdpServer::new(config.clone()).unwrap();

    let (proposal_sink, multi_paxos) = MultiPaxosBuilder::new(config)
        .with_state_machine(register.clone())
        .build();
    server
        .runtime_mut()
        .spawn(client_handler(register, client_addr, proposal_sink));
    server.run(multi_paxos);
}
