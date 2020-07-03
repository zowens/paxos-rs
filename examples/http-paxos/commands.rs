use bincode::{deserialize, serialize};
use bytes::Bytes;
use hyper::{client::HttpConnector, Body, Client, Request};
use paxos::{Ballot, Commander, NodeId, NodeMetadata, Slot, Transport};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
enum Command {
    Proposal(Bytes),
    Prepare(#[serde(with = "BallotDef")] Ballot),
    Promise(NodeId, #[serde(with = "BallotDef")] Ballot, Vec<SlotValueTuple>),
    Accept(#[serde(with = "BallotDef")] Ballot, Vec<(Slot, Bytes)>),
    Reject(NodeId, #[serde(with = "BallotDef")] Ballot, #[serde(with = "BallotDef")] Ballot),
    Accepted(NodeId, #[serde(with = "BallotDef")] Ballot, Vec<Slot>),
    Resolution(#[serde(with = "BallotDef")] Ballot, Vec<(Slot, Bytes)>),
    Catchup(NodeId, Vec<Slot>),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct SlotValueTuple(Slot, #[serde(with = "BallotDef")] Ballot, Bytes);

#[derive(Serialize, Deserialize)]
#[serde(remote = "Ballot")]
struct BallotDef(pub u32, pub u32);

pub struct HttpTransport {
    peers: HashMap<NodeId, PaxosCommander>,
    client: Client<HttpConnector, Body>,
}

impl Default for HttpTransport {
    fn default() -> HttpTransport {
        HttpTransport { client: Client::new(), peers: HashMap::new() }
    }
}

impl Transport for HttpTransport {
    type Commander = PaxosCommander;

    fn send_to<F>(&mut self, node: NodeId, meta: &NodeMetadata, command: F)
    where
        F: FnOnce(&mut Self::Commander) -> (),
    {
        if !self.peers.contains_key(&node) {
            self.peers.insert(
                node,
                PaxosCommander(self.client.clone(), String::from_utf8(meta.0.to_vec()).unwrap()),
            );
        }

        let commander = self.peers.get_mut(&node).unwrap();
        command(commander);
    }
}

pub fn invoke<C: Commander>(replica: &mut C, command: Bytes) {
    let cmd = match deserialize(&command) {
        Ok(cmd) => cmd,
        Err(_) => return,
    };

    match cmd {
        Command::Proposal(val) => replica.proposal(val),
        Command::Prepare(bal) => replica.prepare(bal),
        Command::Promise(node, bal, accepted) => replica.promise(
            node,
            bal,
            accepted.into_iter().map(|SlotValueTuple(slot, bal, val)| (slot, bal, val)).collect(),
        ),
        Command::Accept(bal, vals) => replica.accept(bal, vals),
        Command::Reject(node, proposed, preempted) => replica.reject(node, proposed, preempted),
        Command::Accepted(node, bal, slots) => replica.accepted(node, bal, slots),
        Command::Resolution(bal, vals) => replica.resolution(bal, vals),
        Command::Catchup(node, slots) => replica.catchup(node, slots),
    };
}

pub struct PaxosCommander(Client<HttpConnector, Body>, String);

impl PaxosCommander {
    fn send(&mut self, cmd: Command) {
        let bytes = match serialize(&cmd) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Error serializing command: {:?}", e);
                return;
            }
        };

        let request = Request::builder().method("POST").uri(&self.1).body(bytes.into()).unwrap();
        tokio::spawn(self.0.request(request));
    }
}

impl Commander for PaxosCommander {
    fn proposal(&mut self, val: Bytes) {
        self.send(Command::Proposal(val));
    }

    fn prepare(&mut self, bal: Ballot) {
        self.send(Command::Prepare(bal));
    }

    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>) {
        self.send(Command::Promise(
            node,
            bal,
            accepted.into_iter().map(|(slot, bal, val)| SlotValueTuple(slot, bal, val)).collect(),
        ));
    }

    fn accept(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>) {
        self.send(Command::Accept(bal, values));
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot) {
        self.send(Command::Reject(node, proposed, preempted));
    }

    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>) {
        self.send(Command::Accepted(node, bal, slots));
    }

    fn resolution(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>) {
        self.send(Command::Resolution(bal, values));
    }

    fn catchup(&mut self, node: NodeId, slots: Vec<Slot>) {
        self.send(Command::Catchup(node, slots));
    }
}
