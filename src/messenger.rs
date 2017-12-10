use super::*;

#[allow(unused_variables)]
pub trait Messenger {
    /// PREPARE message (phase 1a) messages propose a new ballot
    fn send_prepare(&self, peer: NodeId, inst: Instance, proposal: Ballot);

    /// PROMISE message (phase 1b) from acceptors promise to not accept ballots
    /// that preceed the proposal from the proposer.
    fn send_promise(
        &self,
        peer: NodeId,
        inst: Instance,
        proposal: Ballot,
        last_accepted: Option<(Ballot, Value)>,
    );

    /// ACCEPT messages (phase 2a) from proposers to acceptors broadcast a value to accept
    /// for an instance.
    fn send_accept(&self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value);

    /// ACCEPTED messages (phase 2b) from acceptors is sent to learners that a value for
    /// an instance has been accepted.
    fn send_accepted(&self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value);

    /// Reject messages (also referred to as NACK) reject a promise or accept message from an
    /// acceptor that has already promise a ballot of that is > the proposal from the peer.
    /// This speeds up consensus rather than relying on retries exclusively.
    fn send_reject(&self, peer: NodeId, inst: Instance, proposal: Ballot, promised: Ballot);

    /// Sync will request the value from an instance
    fn send_sync(&self, peer: NodeId, inst: Instance);

    /// Catchup will send a caught up value from an instance.
    fn send_catchup(&self, peer: NodeId, inst: Instance, current: Value);
}

#[allow(unused_variables)]
pub trait Handler {
    /// Handles PREPARE messages
    fn on_prepare(&mut self, peer: NodeId, inst: Instance, proposal: Ballot);

    /// Handles PROMISE messages
    fn on_promise(
        &mut self,
        peer: NodeId,
        inst: Instance,
        proposal: Ballot,
        last_accepted: Option<(Ballot, Value)>,
    );

    /// Handles ACCEPT messages
    fn on_accept(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value);

    /// Handles ACCEPTED messages
    fn on_accepted(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value);

    /// Handles REJECT messages
    fn on_reject(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, promised: Ballot);

    /// Handles SYNC request messages
    fn on_sync(&mut self, peer: NodeId, inst: Instance);

    /// Handles CATCHUP messages
    fn on_catchup(&mut self, peer: NodeId, inst: Instance, current: Value);
}
