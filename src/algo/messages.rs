use super::{Ballot, NodeId, Value};

/// PREPARE message is the Phase 1a message from a proposer role sent
/// to acceptors to receive agreement to not accept ballots of lower value.
#[derive(Clone, Debug)]
pub struct Prepare(pub NodeId, pub Ballot);

/// ACCEPT message is the Phase 2a message from a proposer role sent
/// to acceptors to accept a value. The ACCEPT message is predicated
/// on the proposer receiving quorum from Phase 1.
#[derive(Clone, Debug)]
pub struct Accept(pub NodeId, pub Ballot, pub Value);

/// Either of the proposer message values.
#[derive(Clone, Debug)]
pub enum ProposerMsg {
    Prepare(Prepare),
    Accept(Accept),
}

/// PROMISE is the Phase 1b message sent from acceptors in reply to
/// PREPARE messages. The ballot in the promise denotes that the acceptor
/// will not accept ballots less than the promised ballot.
#[derive(Clone, Debug)]
pub struct Promise(pub NodeId, pub Ballot, pub Option<(Ballot, Value)>);

/// REJECT is a message sent from an acceptor in reploy to a proposer
/// when a ballot is being proposed or seen in an ACCEPT message that
/// preceeds the last promised value from the acceptor.
#[derive(Clone, Debug)]
pub struct Reject(pub NodeId, pub Ballot, pub Ballot);

/// ACCEPTED is the Phase 2b message that is broadcast from acceptors
/// denoting acceptance of a value.
#[derive(Clone, Debug)]
pub struct Accepted(pub NodeId, pub Ballot, pub Value);

/// RESOLUTION is the result of a quorum of ACCEPTED messages being received.
#[derive(Clone, Debug)]
pub struct Resolution(pub NodeId, pub Ballot, pub Value);
