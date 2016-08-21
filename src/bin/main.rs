extern crate capnp;
extern crate paxos;

use paxos::PaxosMessage;

fn main() {
    let mut v = Vec::new();
    let lv = "hello".bytes().collect::<Vec<_>>();
    PaxosMessage::Accepted("foobarbaz".to_string(), 5, lv).write_to(&mut v).unwrap();
    println!("{}", v.len());
}
