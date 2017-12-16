use super::{Configuration, Instance};
use algo::*;
use messenger::*;

/// Replicated mutable value register, which utilizes Paxos
/// for consensus on the value.
pub struct MultiPaxosValue<M: Messenger> {
    instance: Instance,
    current: PaxosInstance,
    value: Option<Value>,
    config: Configuration,
    messenger: M,
    state_handler: StateHandler,
}

impl<M: Messenger> MultiPaxosValue<M> {
    /// Creates a new multi-paxos register.
    pub fn new(messenger: M, config: Configuration) -> MultiPaxosValue<M> {
        let mut state_handler = StateHandler {};

        match state_handler.load() {
            Some(State {
                instance,
                current_value,
                promised,
                accepted,
            }) => {
                let paxos_inst =
                    PaxosInstance::new(config.current(), config.quorum_size(), promised, accepted);


                MultiPaxosValue {
                    instance,
                    current: paxos_inst,
                    value: current_value,
                    config,
                    messenger,
                    state_handler,
                }
            }
            None => {
                let paxos_inst =
                    PaxosInstance::new(config.current(), config.quorum_size(), None, None);
                MultiPaxosValue {
                    instance: 0,
                    current: paxos_inst,
                    value: None,
                    config,
                    messenger,
                    state_handler,
                }
            }
        }
    }

    /// Moves to the next instance of paxos
    fn advance_instance(&mut self, new_inst: Instance, new_current_value: Option<Value>) {
        self.state_handler.persist(State {
            instance: new_inst,
            current_value: new_current_value,
            promised: None,
            accepted: None,
        });

        self.current =
            PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None);
    }

    /// Broadcasts PREPARE messages to all peers
    fn send_prepare(&mut self, prepare: Prepare) {
        for peer in self.config.peers() {
            self.messenger.send_prepare(peer, self.instance, prepare.1);
        }
    }

    /// Broadcasts ACCEPT messages to all peers
    fn send_accept(&mut self, accept: Accept) {
        for peer in self.config.peers() {
            self.messenger
                .send_accept(peer, self.instance, accept.1, accept.2.clone());
        }
    }

    /// Broadcasts ACCEPTED messages to all peers
    fn send_accepted(&mut self, accepted: Accepted) {
        for peer in self.config.peers() {
            self.messenger
                .send_accepted(peer, self.instance, accepted.1, accepted.2.clone());
        }
    }
}

impl<M: Messenger> Handler for MultiPaxosValue<M> {
    fn on_prepare(&mut self, peer: NodeId, inst: Instance, proposal: Ballot) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.current.receive_prepare(Prepare(peer, proposal)) {
            Ok(Promise(_, ballot, last_accepted)) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.value.clone(),
                    promised: Some(ballot),
                    accepted: last_accepted.clone(),
                });

                self.messenger
                    .send_promise(peer, self.instance, ballot, last_accepted);
            }
            Err(Reject(_, ballot, opposing_ballot)) => {
                self.messenger
                    .send_reject(peer, self.instance, ballot, opposing_ballot);
            }
        }
    }

    fn on_promise(
        &mut self,
        peer: NodeId,
        inst: Instance,
        proposal: Ballot,
        last_accepted: Option<(Ballot, Value)>,
    ) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let promise = Promise(peer, proposal, last_accepted);
        if let Some(accept) = self.current.receive_promise(promise) {
            self.send_accept(accept);
        }
    }

    fn on_reject(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, promised: Ballot) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        // TODO: this returns a value... how do we use it?
        self.current
            .receive_reject(Reject(peer, proposal, promised));
    }

    fn on_accept(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.current.receive_accept(Accept(peer, proposal, value)) {
            Ok(Accepted(_, ballot, value)) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.value.clone(),
                    promised: Some(ballot),
                    accepted: Some((ballot, value.clone())),
                });

                self.messenger
                    .send_accepted(peer, self.instance, ballot, value);
            }
            Err(Reject(_, ballot, opposing_ballot)) => {
                self.messenger
                    .send_reject(peer, self.instance, ballot, opposing_ballot);
            }
        }
    }

    fn on_accepted(&mut self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let resol = self.current
            .receive_accepted(Accepted(peer, proposal, value));

        // if there is quorum, we can advance to the next instance
        if let Some(Resolution(_, _, value)) = resol {
            let new_inst = self.instance + 1;
            self.advance_instance(new_inst, Some(value));
        }
    }

    fn on_sync(&mut self, peer: NodeId, inst: Instance) {
        // receives SYNC request from a peer to get the present value
        // if the instance known to the peer preceeds the current
        // known instance's value
        match self.value {
            Some(ref v) if inst < self.instance => {
                self.messenger.send_catchup(peer, self.instance, v.clone());
            }
            _ => {}
        }
    }

    fn on_catchup(&mut self, peer: NodeId, inst: Instance, current: Value) {
        // only accept a catchup value if it is greater than
        // the current instance known to this node
        if inst > self.instance {
            self.advance_instance(inst, Some(current));
        }
    }
}

struct State {
    instance: Instance,
    current_value: Option<Value>,
    promised: Option<Ballot>,
    accepted: Option<(Ballot, Value)>,
}

struct StateHandler {}

impl StateHandler {
    fn load(&mut self) -> Option<State> {
        None
    }

    fn persist(&mut self, state: State) {
        // Nothing for now
    }
}
