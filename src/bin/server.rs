extern crate env_logger;
extern crate futures;
extern crate log;
extern crate paxos;
extern crate tokio;

use std::sync::{Arc, RwLock};
use bytes::Bytes;
use log::{info, debug, error};
use paxos::{
    Configuration, MultiPaxosBuilder, ProposalSender, ReplicatedState, UdpServer,
    Instance
};
use std::env::args;
use std::io;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

/// Replicated mutable value register
#[derive(Clone)]
pub struct Register {
    value: Arc<RwLock<Option<Bytes>>>,
}

impl Register {
    pub fn new() -> Register {
        Register {
            value: Arc::new(RwLock::new(None)),
        }
    }
}

impl ReplicatedState for Register {
    fn apply_value(&mut self, instance: Instance, value: Bytes) {
        info!(
            "[RESOLUTION] with value at instance {}: {:?}",
            instance, value
        );

        let mut val = self.value.write().unwrap();
        *val = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<Bytes> {
        let val = self.value.read().unwrap();
        val.clone()
    }
}


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
                info!("Get: {:?}", output);
                if let Err(e) = write.write_all(&output).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                }
            }
            Ok(Some(s)) if s.starts_with("propose") => {
                info!("Propose: {}", s);
                let val = match proposals.propose(s.into()) {
                    Ok(_) => "OK",
                    Err(_) => "ERR: Error adding proposal",
                };
                info!("Propose: {}", val);
                if let Err(e) = write.write_all(val.as_bytes()).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                }
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
        if let Err(e) = write.write_all(b"\n").await {
            eprintln!("failed to flush socket; err = {:?}", e);
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
        spawn(async move { client_handler(socket, register, proposals).await});
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
    let server = UdpServer::new(config.clone()).unwrap();

    let (proposal_sink, multi_paxos) = MultiPaxosBuilder::new(config, register.clone()).build();
    let tcp_fut = tcp_listener(register, client_addr, proposal_sink);
    let server_fut = server.run(multi_paxos);
    let (_, _) = tokio::join!(tcp_fut, server_fut);
    Ok(())
}
