use either::Either;
use super::{Ballot, NodeId, Value};

/// `PREPARE` message is the Phase 1a message from a proposer sent
/// to acceptors to receive agreement to not accept ballots of lower value.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Prepare(pub Ballot);

/// `ACCEPT` message is the Phase 2a message from a proposer sent
/// to acceptors to accept a value. The `ACCEPT` message is predicated
/// on the proposer receiving quorum from Phase 1.
#[derive(Clone, Debug)]
pub struct Accept(pub Ballot, pub Value);

/// Either of the proposer message values.
pub type ProposerMsg = Either<Prepare, Accept>;

/// `PROMISE` is the Phase 1b message sent from acceptors in reply to
/// `PREPARE` messages. The ballot in the promise denotes that the acceptor
/// will not accept ballots less than the promised ballot.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Promise(pub Ballot, pub Option<(Ballot, Value)>);

impl Promise {
    /// Creates a reply for the `PROMISE`. Promises are returned to the
    /// proposer that originally sent the `PREPARE`.
    pub(crate) fn reply_to(self, node: NodeId) -> Reply<Promise> {
        Reply {
            reply_to: node,
            message: self,
        }
    }
}

/// `REJECT` is sent from an acceptor in reply to a proposer
/// when a ballot is being proposed in a `PREPARE` message or seen in an
/// `ACCEPT` message that preceeds the last promised value from the acceptor.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Reject(pub Ballot, pub Ballot);

impl Reject {
    /// Creates a reply for a `PROMISE` or `ACCEPT` to the proposer
    /// that originated a message with a ballot that preceeded the last
    /// promised.
    pub(crate) fn reply_to(self, node: NodeId) -> Reply<Reject> {
        Reply {
            reply_to: node,
            message: self,
        }
    }
}


/// `ACCEPTED` is the Phase 2b message that is broadcast from acceptors
/// denoting acceptance of a value.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Accepted(pub Ballot, pub Value);

/// `RESOLUTION` is the result of a quorum of `ACCEPTED` messages being received.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Resolution(pub Ballot, pub Value);

/// Struct containing the node and message for a single destination.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Reply<M> {
    /// Node identifier that should be sent the reply
    pub reply_to: NodeId,
    /// The reply message
    pub message: M,
}
