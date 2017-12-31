//! Messages sent within the cluster of nodes.
use std::net::SocketAddr;

use super::Instance;
pub use algo::{Accept, Accepted, Ballot, Prepare, Promise, Reject};
use algo::NodeId;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
/// Message generated within the cluster.
pub enum MultiPaxosMessage<V> {
    /// `PREPARE` message is the Phase 1a message from a proposer sent
    /// to acceptors to receive agreement to not accept ballots of lower value.
    Prepare(Instance, Prepare),

    /// `PROMISE` is the Phase 1b message sent from acceptors in reply to
    /// `PREPARE` messages. The ballot in the promise denotes that the acceptor
    /// will not accept ballots less than the promised ballot.
    Promise(Instance, Promise<V>),

    /// `ACCEPT` message is the Phase 2a message from a proposer sent
    /// to acceptors to accept a value. The `ACCEPT` message is predicated
    /// on the proposer receiving quorum from Phase 1.
    Accept(Instance, Accept<V>),

    /// `ACCEPTED` is the Phase 2b message that is broadcast from acceptors
    /// denoting acceptance of a value.
    Accepted(Instance, Accepted<V>),

    /// `REJECT` is sent from an acceptor in reply to a proposer
    /// when a ballot is being proposed in a `PREPARE` message or seen in an
    /// `ACCEPT` message that preceeds the last promised value from the acceptor.
    Reject(Instance, Reject),

    /// Request sent to a random node to get the latest value
    /// if the instance known to the node is behind.
    Sync(Instance),

    /// Response to a sync request from another peer
    Catchup(Instance, V),
}

/// Message sent over the network.
pub struct NetworkMessage<V> {
    /// Address of the receiptient or destination of the message
    pub address: SocketAddr,
    /// The message
    pub message: MultiPaxosMessage<V>,
}

/// Message sent to a peer node in the cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMessage<V> {
    /// Address of the receiptient or destination of the message
    pub peer: NodeId,
    /// The message
    pub message: MultiPaxosMessage<V>,
}
