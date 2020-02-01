extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate tokio;

use clap::{App, Arg, SubCommand};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = App::new("paxos client")
        .version("0.1")
        .arg(
            Arg::with_name("node")
                .short("n")
                .global(true)
                .default_value("0")
                .possible_values(&["0", "1", "2"])
                .help("Server node id"),
        )
        .subcommand(
            SubCommand::with_name("propose")
                .about("proposes a value")
                .arg(Arg::with_name("VALUE").required(true).index(1)),
        )
        .subcommand(SubCommand::with_name("get").about("gets the current value"))
        .get_matches();

    let addr: SocketAddr = format!("127.0.0.1:400{}", matches.value_of("node").unwrap_or("0"))
        .parse()
        .unwrap();
    let mut stream = TcpStream::connect(&addr).await?;

    match matches.subcommand() {
        ("propose", Some(args)) => {
            let value = args.value_of("VALUE").unwrap();
            let msg = format!("propose {}\n", value);
            stream.write_all(msg.as_bytes()).await?;
        }
        ("get", Some(_args)) => {
            stream.write_all("get\n".as_bytes()).await?;
            let mut reader = BufReader::new(stream).lines();
            let value = reader.next_line().await?;
            if let Some(v) = value {
                println!("{}", v);
            }
        }
        _ => panic!("UNKNOWN COMMAND"),
    };
    Ok(())
}
