use std::collections::{HashSet};
use super::{Ballot, Value, PaxosMessage};
use std::mem;

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum Role {
    Acceptor,
    PrepareAwaitingPromises,
    // The leader is the distinquished proposer and distinguished learner
    Leader,
}

struct Promises {
    acceptors: HashSet<String>,
    max_value: Option<(Ballot, Value)>,
}

impl Promises {
    pub fn new() -> Promises {
        Promises {
            acceptors: HashSet::new(),
            max_value: None,
        }
    }

    pub fn clear(&mut self) -> Option<(Ballot, Value)> {
        self.acceptors.clear();

        let mut local_mv = None;
        mem::swap(&mut self.max_value, &mut local_mv);
        local_mv
    }

    pub fn len(&self) -> usize {
        self.acceptors.len()
    }

    pub fn insert(&mut self, acceptor: String, last_accepted: Option<(Ballot, Value)>) {
        self.acceptors.insert(acceptor);

        if self.max_value.is_none() {
            self.max_value = last_accepted;
        } else if last_accepted.is_some() {
            let &(bal, _) = self.max_value.as_ref().unwrap();
            let &(bal2, _) = last_accepted.as_ref().unwrap();
            if bal < bal2 {
                self.max_value = last_accepted;
            }
        }
    }
}

pub struct StateMachine {
    identity: String,
    // TODO: configuration struct?
    role: Role,
    quorum: usize,

    // TODO: do we need to track the ballot?
    promises: Promises,

    // TODO: trace last accepted is still tracked in case this
    // ballot gets rejected with NACK and we step
    // back into an acceptor role
    last_accepted: Option<(Ballot, Value)>,

    max_ballot: Option<Ballot>,
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Step {
    Reply(PaxosMessage),
    Broadcast(PaxosMessage),
    NoAction,
}

impl StateMachine {
    pub fn new(identity: String, quorum: usize) -> StateMachine {
        StateMachine {
            identity: identity,
            role: Role::Acceptor,
            promises: Promises::new(),
            last_accepted: None,
            quorum: quorum,
            max_ballot: None,
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn last_accepted(&self) -> Option<(Ballot, Value)> {
        self.last_accepted.clone()
    }

    // Sends prepare messages to acceptors. Since we're running
    // multi-paxos, this will also grant leadership at ballot b
    // once a quorum is reached.
    pub fn prepare(&mut self) -> Step {
        // ignore if we're already the leader or already pending leadership
        match self.role {
            Role::Leader | Role::PrepareAwaitingPromises => Step::NoAction,
            Role::Acceptor => {
                // TODO: pick a unique sequence value

                // Phase1(a) A proposer selects a proposal number n and sends a prepare
                // request with number n to a majority of acceptors.
                // grab the next ballot number
                let ballot = self.max_ballot.map(|b| b + 1).unwrap_or(0);
                self.max_ballot = Some(ballot);
                self.promises.clear();
                self.role = Role::PrepareAwaitingPromises;

                // send the prepare request
                Step::Broadcast(PaxosMessage::Prepare(ballot))
            }
        }
    }

    pub fn propose(&mut self, v: Vec<u8>) -> Option<Step> {
        match self.role {
            Role::Leader => {
                self.last_accepted = Some((self.max_ballot.unwrap(), v.clone()));
                Some(Step::Broadcast(PaxosMessage::Accept(self.max_ballot.unwrap(), v)))
            },
            _ => None
        }
    }

    pub fn handle(&mut self, msg: PaxosMessage) -> Step {
        println!("Handling msg");
        match (self.role, msg) {
            // Phase1(b) If an acceptor receives a prepare request with number n greater
            // than that of any prepare request to which it has already responded,
            // then it responds to the request with a promise not to accept any more
            // proposals numbered less than n and with the highest-numbered proposal
            // (if any) that it has accepted.
            (_, PaxosMessage::Prepare(b)) => {
                if self.max_ballot.map(|l| b > l).unwrap_or(true) {
                    self.max_ballot = Some(b);
                    self.role = Role::Acceptor;
                    // TODO: get rid of the clones
                    Step::Reply(PaxosMessage::Promise(self.identity.clone(), b, self.last_accepted.clone()))
                } else {
                    Step::NoAction
                }
            },
            // Phase2(a) If the proposer receives a response to its prepare requests
            // (numbered n) from a majority of acceptors, then it sends an accept
            // request to each of those acceptors for a proposal numbered n with a
            // value v, where v is the value of the highest-numbered proposal among
            // the responses, or is any value if the responses reported no proposals.
            (Role::PrepareAwaitingPromises, PaxosMessage::Promise(acceptor, b, last_accepted)) => {
                // TODO: ensure ballot matches

                self.promises.insert(acceptor, last_accepted);
                if self.promises.len() >= self.quorum {
                    // sending out Accept messages when quorum reached
                    self.role = Role::Leader;
                    let max_accepted = self.promises.clear();
                    if let Some(v) = max_accepted {
                        Step::Broadcast(PaxosMessage::Accept(b, v.1))
                    } else {
                        Step::NoAction
                    }
                } else {
                    Step::NoAction
                }
            },
            // Phase2(b) If an acceptor receives an accept request for a proposal numbered
            // n, it accepts the proposal unless it has already responded to a prepare
            // request having a number greater than n.
            (_, PaxosMessage::Accept(b, v)) => {
                // TODO: what if we're leader or waiting?
                // TODO: NACK?

                match self.max_ballot {
                    Some(mb) if mb == b => {
                        self.last_accepted = Some((b, v.clone()));
                        Step::Reply(PaxosMessage::Accepted(self.identity.clone(), b, v))
                    },
                    _ => Step::NoAction,
                }
            },
            (Role::Leader, PaxosMessage::Accepted(acceptor, ballot, value)) => {
                // TODO:
                Step::NoAction
            },
            _ => Step::NoAction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{PaxosMessage};

    #[test]
    fn test_prepare() {
        let mut machine = StateMachine::new("s1".to_string(), 1);
        assert_eq!(Role::Acceptor, machine.role());

        // valid proposal, first one
        let step1 = machine.handle(PaxosMessage::Prepare(1));
        assert_eq!(Step::Reply(PaxosMessage::Promise("s1".to_string(), 1, None)), step1);

        // proposal is < previous
        let step2 = machine.handle(PaxosMessage::Prepare(0));
        assert_eq!(Step::NoAction, step2); //< TODO: NACK

        // proposal is > previous
        let step3 = machine.handle(PaxosMessage::Prepare(2));
        assert_eq!(Step::Reply(PaxosMessage::Promise("s1".to_string(), 2, None)), step3);
    }

    #[test]
    fn test_proposer() {
        let mut proposer = StateMachine::new("s1".to_string(), 2);
        assert_eq!(Role::Acceptor, proposer.role());

        // broadcasts proposal
        let proposal_step = proposer.prepare();
        assert_eq!(Step::Broadcast(PaxosMessage::Prepare(0)), proposal_step);
        assert_eq!(Role::PrepareAwaitingPromises, proposer.role());

        // first acceptor accepts proposal, must have 2
        let first_accepted = proposer.handle(PaxosMessage::Promise("s2".to_string(), 0, None));
        assert_eq!(Step::NoAction, first_accepted);
        assert_eq!(Role::PrepareAwaitingPromises, proposer.role());

        // second acceptor accepts proposal
        let second_accepted = proposer.handle(PaxosMessage::Promise("s3".to_string(), 0, None));

        // since the acceptors do not have values, we don't sent out an "Accept" message
        // quite yet... we wait for a value to come in
        assert_eq!(Step::NoAction, second_accepted);
        assert_eq!(Role::Leader, proposer.role());

        // lagging acceptor results in no action
        let third_accepted = proposer.handle(PaxosMessage::Promise("s4".to_string(), 0, None));
        assert_eq!(Step::NoAction, third_accepted);
    }

    #[test]
    fn test_acceptor() {
        let mut acceptor = StateMachine::new("s2".to_string(), 2);
        assert_eq!(Role::Acceptor, acceptor.role());
        assert_eq!(None, acceptor.last_accepted());

        let first_prepare = acceptor.handle(PaxosMessage::Prepare(5));
        assert_eq!(Step::Reply(PaxosMessage::Promise("s2".to_string(), 5, None)), first_prepare);

        // prepare before last prepare (4 < 5)
        let violated_prepare = acceptor.handle(PaxosMessage::Prepare(4));
        // TODO: change to NACK
        assert_eq!(Step::NoAction, violated_prepare);

        // accept ballot 5 value
        let accept_resp = acceptor.handle(PaxosMessage::Accept(5, vec![2u8]));
        assert_eq!(Step::Reply(PaxosMessage::Accepted("s2".to_string(), 5, vec![2u8])), accept_resp);
        assert_eq!(Some((5, vec![2u8])), acceptor.last_accepted());
    }
}
