use crate::{Ballot, NodeId, NodeMetadata, Slot};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Sends commands to other replicas in addition to applying
/// resolved commands at the current replica
pub trait Transport {
    /// Send a message to a single node
    fn send(&mut self, node: NodeId, node_metadata: &NodeMetadata, command: Command);
}

/// Receiver of Paxos commands.
pub trait Receiver {
    /// Receives a command and reacts accordingly
    fn receive(&mut self, command: Command);
}

/// Receiver of Paxos commands.
///
/// This is a convenience trait that breaks out reactors for each command.
pub trait Commander {
    /// Receive a proposal
    fn proposal(&mut self, val: Bytes);

    /// Receive a Phase 1a PREPARE message containing the proposed ballot
    fn prepare(&mut self, bal: Ballot);

    /// Receive a Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and all accepted
    /// values within the open window.
    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>);

    /// Receive a Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    fn accept(&mut self, bal: Ballot, slot_values: Vec<(Slot, Bytes)>);

    /// Receives a REJECT message from a peer containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot);

    /// Receives a Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>);

    /// Receives a final resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    fn resolution(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>);

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    fn catchup(&mut self, node: NodeId, slots: Vec<Slot>);
}

impl<T: Commander> Receiver for T {
    fn receive(&mut self, command: Command) {
        match command {
            Command::Proposal(val) => {
                self.proposal(val);
            }
            Command::Prepare(bal) => {
                self.prepare(bal);
            }
            Command::Promise(node, bal, accepted) => {
                self.promise(node, bal, accepted);
            }
            Command::Accept(bal, slot_vals) => {
                self.accept(bal, slot_vals);
            }
            Command::Reject(node, proposed, preempted) => {
                self.reject(node, proposed, preempted);
            }
            Command::Accepted(node, bal, slots) => {
                self.accepted(node, bal, slots);
            }
            Command::Resolution(bal, slot_vals) => {
                self.resolution(bal, slot_vals);
            }
            Command::Catchup(node, slots) => {
                self.catchup(node, slots);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// RPC commands sent between replicas
pub enum Command {
    /// Propose a value
    Proposal(Bytes),

    /// Phase 1a PREPARE message containing the proposed ballot
    Prepare(Ballot),

    /// Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and all accepted
    /// values within the open window.
    Promise(NodeId, Ballot, Vec<(Slot, Ballot, Bytes)>),

    /// Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    Accept(Ballot, Vec<(Slot, Bytes)>),

    /// REJECT a peer's previous message containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    Reject(NodeId, Ballot, Ballot),

    /// Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    Accepted(NodeId, Ballot, Vec<Slot>),

    /// Resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    Resolution(Ballot, Vec<(Slot, Bytes)>),

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    Catchup(NodeId, Vec<Slot>),
}
