use super::Slot;
use crate::config::NodeId;
use crate::paxos::Ballot;
use bytes::Bytes;
use std::iter::Extend;

/// Tuple containing the the slot number, ballot and value
pub type SlottedValue = (Slot, Ballot, Bytes);

/// Sends commands to other replicas
pub trait Sender {
    /// Commander type used to send messages to other instances
    type Commander: Commander;

    /// Send a message to a single node
    fn send_to<F>(&mut self, node: NodeId, command: F)
    where
        F: FnOnce(&mut Self::Commander) -> ();
}

#[derive(PartialEq, Eq, Debug)]
pub enum Command {
    Proposal(Bytes),
    Prepare(Ballot),
    Promise(NodeId, Ballot, Vec<(Slot, Ballot, Bytes)>),
    Accept(Slot, Ballot, Bytes),
    Reject(NodeId, Ballot, Ballot),
    Accepted(NodeId, Slot, Ballot),
    Resolution(Slot, Ballot, Bytes),
}

pub trait Commander {
    fn proposal(&mut self, val: Bytes);
    fn prepare(&mut self, bal: Ballot);
    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<SlottedValue>);
    fn accept(&mut self, slot: Slot, bal: Ballot, val: Bytes);
    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot);
    fn accepted(&mut self, node: NodeId, slot: Slot, bal: Ballot);
    fn resolution(&mut self, slot: Slot, bal: Ballot, val: Bytes);
}

impl<T> Commander for T
where
    T: Extend<Command>,
{
    fn proposal(&mut self, bytes: Bytes) {
        self.extend(Some(Command::Proposal(bytes)));
    }

    fn prepare(&mut self, bal: Ballot) {
        self.extend(Some(Command::Prepare(bal)));
    }

    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<SlottedValue>) {
        self.extend(Some(Command::Promise(node, bal, accepted)));
    }

    fn accept(&mut self, slot: Slot, bal: Ballot, val: Bytes) {
        self.extend(Some(Command::Accept(slot, bal, val)));
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, promised: Ballot) {
        self.extend(Some(Command::Reject(node, proposed, promised)));
    }

    fn accepted(&mut self, node: NodeId, slot: Slot, bal: Ballot) {
        self.extend(Some(Command::Accepted(node, slot, bal)));
    }

    fn resolution(&mut self, slot: Slot, bal: Ballot, val: Bytes) {
        self.extend(Some(Command::Resolution(slot, bal, val)));
    }
}

#[derive(Default)]
pub struct EmptySender;

impl Sender for EmptySender {
    type Commander = EmptyCommander;

    fn send_to<F>(&mut self, _: NodeId, f: F)
    where
        F: FnOnce(&mut Self::Commander) -> (),
    {
        let mut commander = EmptyCommander::default();
        f(&mut commander);
    }
}

#[derive(Default)]
pub struct EmptyCommander;
impl Commander for EmptyCommander {
    fn proposal(&mut self, _: Bytes) {}
    fn prepare(&mut self, _: Ballot) {}
    fn promise(&mut self, _: NodeId, _: Ballot, _: Vec<SlottedValue>) {}
    fn accept(&mut self, _: Slot, _: Ballot, _: Bytes) {}
    fn reject(&mut self, _: NodeId, _: Ballot, _: Ballot) {}
    fn accepted(&mut self, _: NodeId, _: Slot, _: Ballot) {}
    fn resolution(&mut self, _: Slot, _: Ballot, _: Bytes) {}
}
