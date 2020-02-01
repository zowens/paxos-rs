extern crate env_logger;
extern crate futures;
extern crate log;
extern crate paxos;
extern crate tokio;

use bytes::Bytes;
use log::{debug, error};
use paxos::{
    Configuration, MultiPaxosBuilder, ProposalSender, Register, ReplicatedState, UdpServer,
};
use std::env::args;
use std::io;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

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

async fn client_handler(
    stream: TcpStream,
    register: Register,
    proposals: ProposalSender,
) -> io::Result<()> {
    let (read, mut write) = split(stream);
    let mut lines = BufReader::new(read).lines();

    loop {
        match lines.next_line().await {
            Ok(None) => {
                debug!("No line added");
                continue;
            }
            Ok(Some(s)) if s.starts_with("get") => {
                let output = match register.snapshot(0) {
                    Some(v) => v,
                    None => Bytes::from_static("ERR: No Value".as_bytes()),
                };
                write.write_all(&output).await?;
            }
            Ok(Some(s)) if s.starts_with("propose") => {
                let val = match proposals.propose(s.into()) {
                    Ok(_) => "OK",
                    Err(_) => "ERR: Error adding proposal",
                };
                write.write_all(val.as_bytes()).await?;
            }
            Ok(Some(s)) => {
                let err_str = format!("ERR: invalid command {}", s);
                write.write_all(err_str.as_bytes()).await?;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => {
                error!("ERROR: {}", e);
                return Err(e);
            }
        }
    }
}

/// Allow clients to send messages to issue commands to the node:
///
/// Examples:
/// * `get`
/// * `propose hello world`
async fn tcp_listener(
    register: Register,
    addr: SocketAddr,
    proposals: ProposalSender,
) -> io::Result<()> {
    let mut listener = TcpListener::bind(&addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;

        let register = register.clone();
        let proposals = proposals.clone();
        spawn(async move { client_handler(socket, register, proposals).await });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

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
        .spawn(tcp_listener(register, client_addr, proposal_sink));
    server.run(multi_paxos);
    Ok(())
}
