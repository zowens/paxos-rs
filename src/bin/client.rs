extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;

use clap::{App, Arg, SubCommand};
use futures::{Future, Sink, Stream};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_codec::{Decoder, LinesCodec};

fn main() {
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
    let connect = TcpStream::connect(&addr).map(|conn| LinesCodec::new().framed(conn).split());

    match matches.subcommand() {
        ("propose", Some(args)) => {
            let value = args.value_of("VALUE").unwrap();
            let msg = format!("propose {}", value).to_string();
            let req_future = connect
                .and_then(move |(sink, _)| sink.send(msg))
                .map(|_| ())
                .map_err(|e| println!("ERROR: {}", e));
            req_future.wait().unwrap()
        }
        ("get", Some(_args)) => {
            let req_future = connect
                .and_then(move |(sink, stream)| {
                    sink.send("get".to_string()).and_then(|_| {
                        stream.take(1).for_each(move |msg| {
                            println!("{}", msg);
                            Ok(())
                        })
                    })
                })
                .map_err(|e| println!("ERROR: {}", e));
            req_future.wait().unwrap()
        }
        _ => panic!("UNKNOWN COMMAND"),
    };
}
