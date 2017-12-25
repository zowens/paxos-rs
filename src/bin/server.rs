#![feature(ip_constructors)]
extern crate paxos;
extern crate futures;
extern crate env_logger;

use std::net::SocketAddr;
use std::net::Ipv4Addr;

use std::env::args;
use paxos::{Configuration, UdpServer, Register, FuturesScheduler, MultiPaxos};

fn local_config(node: u16) -> (Configuration, SocketAddr) {
    assert!(node < 3);

    let ip = Ipv4Addr::localhost().into();
    let current = (node as u64, SocketAddr::new(ip, 3000 + node));
    let client_addr = SocketAddr::new(ip, 4000 + node);
    let others = (0..3u16).filter(|n| *n != node).map(|n| (n as u64, SocketAddr::new(ip, 3000 + n)));
    (Configuration::new(current, others), client_addr)
}

pub fn main() {
    env_logger::init().unwrap();

    let (config, client_addr) = match args().nth(1) {
        Some(v) => local_config(v.parse::<u16>().unwrap()),
        None => panic!("Must specific node ID (0, 1, 2) in first argument"),
    };

    println!("{:?}", config);

    let server = UdpServer::new(config.current_address(), &client_addr).unwrap();
    let multi_paxos = MultiPaxos::new(Register::default(), FuturesScheduler::default(), config);
    server.run(multi_paxos).unwrap();
}
