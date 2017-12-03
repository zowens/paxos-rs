use std::collections::HashMap;
use std::mem;
use super::*;

/// PREPARE message is the Phase 1a message from a proposer role sent
/// to acceptors to receive agreement to not accept ballots of lower value.
#[derive(Debug)]
pub struct Prepare(NodeId, Ballot);

/// ACCEPT message is the Phase 2a message from a proposer role sent
/// to acceptors to accept a value. The ACCEPT message is predicated
/// on the proposer receiving quorum from Phase 1.
#[derive(Debug)]
pub struct Accept(NodeId, Ballot, Value);

/// Either of the proposer message values.
#[derive(Debug)]
pub enum ProposerMsg {
    Prepare(Prepare),
    Accept(Accept),
}

/// PROMISE is the Phase 1b message sent from acceptors in reply to
/// PREPARE messages. The ballot in the promise denotes that the acceptor
/// will not accept ballots less than the promised ballot.
#[derive(Debug)]
pub struct Promise(NodeId, Ballot, Option<(Ballot, Value)>);

/// REJECT is a message sent from an acceptor in reploy to a proposer
/// when a ballot is being proposed or seen in an ACCEPT message that
/// preceeds the last promised value from the acceptor.
#[derive(Debug)]
pub struct Reject(NodeId, Ballot, Ballot);

/// ACCEPTED is the Phase 2b message that is broadcast from acceptors
/// denoting acceptance of a value.
#[derive(Debug)]
pub struct Accepted(NodeId, Ballot, Value);

/// RESOLUTION is the result of a quorum of ACCEPTED messages being received.
#[derive(Debug)]
pub struct Resolution(NodeId, Ballot, Value);


/// Encoding of the Proposer role's state machine
#[derive(Debug)]
enum ProposerState {
    /// Empty state: this proposer is not a candidate (in Phase 1) or
    /// a leader (Phase 2)
    Empty,
    /// Proposer has sent out Phase 1a messages to acceptors to become leader.
    Candidate {
        /// The ballot sent out with the PREPARE message
        proposal: Ballot,
        /// Value that will be sent with the first ACCEPT message. This is either
        /// a proposed value initially or will be overwritten by the highest accepted
        /// value from the acceptor with the highest ballot.
        value: Option<Value>,
        /// Tracking the PROMISE messages received by acceptors.
        promises: QuorumSet,
        /// Tracking of the REJECT messages received by acceptors.
        promise_rejections: QuorumSet,
        /// Ballot from the acceptor who sent a PROMISE that is the highest seen.
        highest_accepted: Option<Ballot>,
    },
    /// Proposer has received quorum of PROMISE messages and has entered Phase 2
    /// and has a master lease.
    Leader {
        /// The ballot to send with ACCEPT messages
        proposal: Ballot,
        /// Value to send via ACCEPT
        value: Option<Value>,
        /// Rejections received from acceptors
        rejections: QuorumSet,
    },
}

impl ProposerState {
    fn take_value(&mut self) -> Option<Value> {
        match *self {
            ProposerState::Empty => None,
            ProposerState::Candidate { ref mut value, .. }
            | ProposerState::Leader { ref mut value, .. } => value.take(),
        }
    }
}

/// The proposer is a role within paxos that acts as a coordinator for the instance
/// in that it attempts to elect itself the proposer (leader) for the instance via
/// Phase 1. Once it has received a quorum, it will move to Phase 2 in which is will
/// potentially send an ACCEPT message with a value from the acceptor with the highest
/// accepted value already seen (key to the Paxos algorithm)
struct Proposer {
    /// State of the proposer state machine
    state: ProposerState,
    /// Highest seen ballot thus far from any peer
    highest: Option<Ballot>,
    /// Node ID of the current node (used to construct ballots)
    current: NodeId,
    /// Number of nodes for quorum
    quorum: usize,
}

impl Proposer {
    /// Overrides the highest seen value, if ballot is the highest seen
    fn observe_ballot(&mut self, ballot: Ballot) {
        // empty OR existing highest < ballot causes highest to be set
        if self.highest.is_none() || self.highest.filter(|b| b < &ballot).is_some() {
            trace!("Proposer observed higher {:?}", ballot);
            self.highest = Some(ballot);
        }
    }

    /// Prepare sets state to candidate and begins to track promises.
    fn prepare(&mut self, value: Option<Value>) -> Prepare {
        let new_ballot = self.highest
            .map(|m| m.higher_for(self.current))
            .unwrap_or_else(|| Ballot(0, self.current));

        self.highest = Some(new_ballot);

        self.state = ProposerState::Candidate {
            proposal: new_ballot,
            promises: QuorumSet::with_size(self.quorum),
            promise_rejections: QuorumSet::with_size(self.quorum),
            highest_accepted: None,
            value,
        };

        debug!("Starting prepare with {:?}", new_ballot);

        Prepare(self.current, new_ballot)
    }

    /// Proposes a value once Phase 1 is completed. It is not guaranteed that the value will
    /// be accepted, as the maximum accepted value from acceptors is used as preference.
    ///
    /// TODO: Do we need to note if the value is not used?
    fn propose_value(&mut self, v: Value) -> Option<ProposerMsg> {
        match self.state {
            // if the proposer is in the empty state, we'll attempt to run Phase 1
            // with a prepare message
            ProposerState::Empty => Some(ProposerMsg::Prepare(self.prepare(Some(v)))),
            // if we're a candidate and the value is not already set, set the value
            // (still waiting on quorum to start Phase 2)
            ProposerState::Candidate { ref mut value, .. } if value.is_none() => {
                *value = Some(v);
                None
            }
            // if we're in a leader state and a value has not already been accepted or
            // proposed, then set the value and send the accept message.
            ProposerState::Leader {
                ref mut value,
                proposal,
                ..
            } if value.is_none() =>
            {
                *value = Some(v.clone());
                Some(ProposerMsg::Accept(Accept(self.current, proposal, v)))
            }
            _ => None,
        }
    }

    /// Handler for REJECT from an acceptor peer. Phase 1 with a higher ballot is returned
    /// if the rejection has quorum.
    fn receive_reject(&mut self, reject: Reject) -> Option<Prepare> {
        let Reject(peer, proposed, promised) = reject;
        debug!(
            "Received REJECT for {:?} with greater {:?} from peer {}",
            proposed,
            promised,
            peer
        );
        assert!(
            proposed < promised,
            "Ballot received in REJECT was >= proposed"
        );

        self.observe_ballot(promised);

        let lost_leadership = match self.state {
            ProposerState::Candidate {
                ref mut promise_rejections,
                proposal,
                ..
            } if proposal == proposed =>
            {
                promise_rejections.insert(peer);
                promise_rejections.has_quorum()
            }
            ProposerState::Leader {
                ref mut rejections,
                proposal,
                ..
            } if proposal == proposed =>
            {
                rejections.insert(peer);
                rejections.has_quorum()
            }
            _ => false,
        };
        // TODO: should we actually send out another prepare here?
        if lost_leadership {
            debug!("Lost leadership, restarting prepare");
            let value = self.state.take_value();
            Some(self.prepare(value))
        } else {
            None
        }
    }

    /// Note a promise from a peer. An ACCEPT message is returned if quorum is detected.
    fn receive_promise(&mut self, promise: Promise) -> Option<Accept> {
        let Promise(peer, proposed, accepted) = promise;
        debug!("Received PROMISE for {:?} from peer {}", proposed, peer);

        self.observe_ballot(proposed);

        match self.state {
            // if a promise is seen in the candiate state, we check for quorum to enter Phase 2
            ProposerState::Candidate {
                proposal,
                ref mut promises,
                ref mut highest_accepted,
                ref mut value,
                ..
            } if proposal == proposed && !promises.contains(peer) =>
            // only allow matching proposals (we could have restarted Phase 1) and only update when
            // we see a new promise from a new peer
            {
                trace!("New promise from peer received");
                promises.insert(peer);

                // override the proposed value to send on the accept if the highest_accept is <
                // this promise's last accepted ballot value
                if let Some((bal, v)) = accepted {
                    let set = match *highest_accepted {
                        Some(ref b) => b < &bal,
                        _ => true,
                    };
                    if set {
                        trace!("Peer has the highest accepted value thus far");
                        *highest_accepted = Some(bal);
                        *value = Some(v);
                    }
                }

                if !promises.has_quorum() {
                    return None;
                }
            }
            _ => {
                return None;
            }
        };

        debug!("Quorum reached for Phase 1 of {:?}", proposed);

        // proposer has quorum from acceptors, upgrade to Leader and start
        // Phase 2 if we already have a value

        let mut s = ProposerState::Empty;
        mem::swap(&mut s, &mut self.state);

        match s {
            ProposerState::Candidate {
                proposal,
                promise_rejections,
                value,
                ..
            } => {
                let accept = value.clone().map(|v| Accept(self.current, proposal, v));

                self.state = ProposerState::Leader {
                    proposal,
                    rejections: promise_rejections,
                    value,
                };

                accept
            }
            _ => unreachable!("Already know its a candidate transitioning to leader"),
        }
    }
}

/// Encoding of Acceptor (persistent Paxos memory) role
struct Acceptor {
    /// current node identifier
    current: NodeId,
    /// last promised ballot within this instance
    promised: Option<Ballot>,
    /// last accepted ballot/value pair within this instance
    accepted: Option<(Ballot, Value)>,
}

impl Acceptor {
    /// Handler for a PREPARE message sent from a proposer. The result is either a PROMISE
    /// to the proposer to not accept ballots > proposal or a REJECT if a ballot has been
    /// promised with a ballot > proposal.
    fn receive_prepare(&mut self, prepare: Prepare) -> Result<Promise, Reject> {
        let Prepare(_peer, proposal) = prepare;
        let opposing_ballot = self.promised.filter(|b| b > &proposal);

        match opposing_ballot {
            Some(b) => {
                debug!("Rejecting proposed {:?} with greater {:?}", proposal, b);
                Err(Reject(self.current, proposal, b))
            }
            None => {
                debug!("Promising {:?}", proposal);

                // track the proposal as the highest promise
                // (in order to reject ballots < proposal)
                self.promised = Some(proposal);
                Ok(Promise(self.current, proposal, self.accepted.clone()))
            }
        }
    }

    /// Handler for an ACCEPT message, which is sent from a proposer when a quorum
    /// for the Phase 1 PREPARE has been made from acceptors. Opposing ballots may still
    /// happen in Phase 2, in which case a REJECT is sent.
    fn receive_accept(&mut self, accept: Accept) -> Result<Accepted, Reject> {
        let Accept(_peer, proposal, value) = accept;
        let opposing_ballot = self.promised.filter(|b| b > &proposal);
        match opposing_ballot {
            Some(b) => {
                debug!(
                    "Rejecting ACCEPT message with ballot {:?} because of greater {:?}",
                    proposal,
                    b
                );
                Err(Reject(self.current, proposal, b))
            }
            None => {
                debug!("Accepting proposal {:?}", proposal);

                // set the accepted value, which is sent to subsequent PROMISE responses
                // with ballts greater than the current proposal
                self.accepted = Some((proposal, value.clone()));

                // set the promised value accordingly. In Paxos, it is possible
                // for an acceptor to miss the PREPARE (as in, not participate in quorum)
                // yet still participate in Phase 2 quorum. Once this is the case, we need
                // to ensure that future PREPARE messages from Proposers will not be lower
                // than the accepted value from this ACCEPT message.
                self.promised = Some(proposal);
                Ok(Accepted(self.current, proposal, value))
            }
        }
    }
}


/// Tracking of the proposal within the learner state machien
#[derive(Debug)]
struct ProposalStatus {
    /// Set of acceptors that have sent ACCEPTED responses for this instance
    acceptors: QuorumSet,
    /// Value of the value from the acceptors (the invariant is that all
    /// acceptors will send the same value for a given ballot)
    value: Value,
}

/// State machine for the learner
#[derive(Debug)]
enum LearnerState {
    /// The learner is waiting for ACCEPTED messages from the acceptors to
    /// meet quorum
    AwaitQuorum {
        /// maps ballots to status (for quorum tracking). it is possible
        /// for acceptors to send out ACCEPTED for different ballots,
        /// thus we need to wait for the final ballot's quorum to proceed
        /// to the final state
        proposals: HashMap<Ballot, ProposalStatus>,

        /// holds mapping of acceptor's last ballot in order to Reject
        /// previous ballot acceptance
        acceptors: HashMap<NodeId, Ballot>,
    },
    /// A final value has been chosen by a quorum of acceptors
    Final {
        /// Final ballot that was accepted
        accepted: Ballot,
        /// Accepted value
        value: Value,
        /// Acceptors that sent ACCEPTED.
        ///
        /// (INVARIANT: acceptors will be a quorum)
        acceptors: QuorumSet,
    },
}

/// Handler for state transitions for the Learner role. A Paxos Learner listens
/// for ACCEPTED messages in order to determine quorum for the final value.
struct Learner {
    /// state of the learner (AwaitQuorum or Final)
    state: LearnerState,
    /// current node identifier
    current: NodeId,
    /// Size of quorum
    quorum: usize,
}

impl Learner {
    /// Handles ACCEPTED messages from acceptors.
    fn receive_accepted(&mut self, accepted: Accepted) -> Option<Resolution> {
        let Accepted(peer, proposal, value) = accepted;

        let final_acceptors = match self.state {
            // a learner awaiting quorum is waiting for a majority
            // of acceptors to finish Phase 2 and send the ACCEPTED
            // message (Phase 2b)
            LearnerState::AwaitQuorum {
                ref mut proposals,
                ref mut acceptors,
            } => {
                use std::collections::hash_map::Entry::*;

                // update the latest ballot for the peer. it is
                // possible to receive multiple ACCEPTED messages
                // from a single acceptor if quorum for Phase 2
                // not reached with previous ballots
                match acceptors.entry(peer) {
                    Occupied(mut e) => {
                        // if this is an older ballot, discard it
                        if *e.get() >= proposal {
                            trace!("Ignoring outdated {:?}", proposal);
                            return None;
                        }

                        let prev = e.insert(proposal);

                        // remove the acceptor's old proposal from the
                        // set of proposals
                        if let Occupied(mut e) = proposals.entry(prev) {
                            trace!("Dropping previous proposal from this acceptor");
                            let remove = {
                                let v = e.get_mut();
                                v.acceptors.remove(peer);
                                v.acceptors.is_empty()
                            };

                            // remove the entry from the hashmap if the
                            // ballot has been superseded
                            if remove {
                                e.remove();
                            }
                        } else {
                            panic!("Proposal not found in the set of proposals already seen");
                        }
                    }
                    // new ACCEPTED from this acceptor
                    Vacant(mut e) => {
                        e.insert(proposal);
                    }
                }

                // insert the ACCEPTED as part of the ballot
                debug!("ACCEPTED for {:?}", proposal);
                let quorum = self.quorum;
                let mut proposal_status = proposals.entry(proposal).or_insert_with(|| {
                    ProposalStatus {
                        acceptors: QuorumSet::with_size(quorum),
                        value: value.clone(),
                    }
                });

                assert_eq!(
                    value,
                    proposal_status.value,
                    "Values for acceptor value does not match value from ACCEPTED message"
                );
                proposal_status.acceptors.insert(peer);

                // if learner has has quorum of ACCEPTED messages, transition to final
                // otherwise no resolution has been reached
                if proposal_status.acceptors.has_quorum() {
                    proposal_status.acceptors.clone()
                } else {
                    return None;
                }
            }
            LearnerState::Final {
                accepted,
                ref value,
                ref mut acceptors,
            } => {
                assert!(
                    acceptors.has_quorum(),
                    "A quorum should have been reached for final value"
                );

                // TODO: why is this >= and not ==?
                if proposal >= accepted {
                    acceptors.insert(peer);
                }
                return Some(Resolution(self.current, accepted, value.clone()));
            }
        };

        debug!("Quorum reached for Phase 2 {:?}", proposal);

        // a final value has been selected, move the state to final
        self.state = LearnerState::Final {
            acceptors: final_acceptors,
            value: value.clone(),
            accepted: proposal,
        };

        Some(Resolution(self.current, proposal, value))
    }
}

/// Instance of the Paxos algorithm.
pub struct PaxosInstance {
    proposer: Proposer,
    acceptor: Acceptor,
    learner: Learner,
}

impl PaxosInstance {
    /// Creates a new instance of Paxos for a given node.
    pub fn new(current: NodeId, quorum: usize) -> PaxosInstance {
        PaxosInstance {
            proposer: Proposer {
                state: ProposerState::Empty,
                highest: None,
                current,
                quorum,
            },
            acceptor: Acceptor {
                current,
                promised: None,
                accepted: None,
            },
            learner: Learner {
                state: LearnerState::AwaitQuorum {
                    proposals: HashMap::new(),
                    acceptors: HashMap::new(),
                },
                current,
                quorum,
            },
        }
    }

    /// Starts prepare to begin Phase 1.
    pub fn prepare(&mut self) -> Prepare {
        self.proposer.prepare(None)
    }

    /// Proposes a value once Phase 1 is completed. It is not guaranteed that the value will
    /// be accepted, as the maximum accepted value from acceptors is used as preference.
    ///
    /// TODO: Do we need to note if the value is not used?
    fn propose_value(&mut self, v: Value) -> Option<ProposerMsg> {
        self.proposer.propose_value(v)
    }

    /// Handler for a PROMISE from a peer. An ACCEPT message is returned if quorum is detected.
    pub fn receive_promise(&mut self, promise: Promise) -> Option<Accept> {
        self.proposer.receive_promise(promise)
    }

    /// Handler for REJECT from an acceptor peer. Phase 1 with a higher ballot is returned
    /// if the rejection has quorum.
    pub fn receive_reject(&mut self, reject: Reject) -> Option<Prepare> {
        self.proposer.receive_reject(reject)
    }

    /// Handler for a PREPARE message sent from a proposer. The result is either a PROMISE
    /// to the proposer to not accept ballots > proposal or a REJECT if a ballot has been
    /// promised with a ballot > proposal.
    pub fn receive_prepare(&mut self, prepare: Prepare) -> Result<Promise, Reject> {
        self.proposer.observe_ballot(prepare.1);
        self.acceptor.receive_prepare(prepare)
    }

    /// Handler for an ACCEPT message, which is sent from a proposer when a quorum
    /// for the Phase 1 PREPARE has been made from acceptors. Opposing ballots may still
    /// happen in Phase 2, in which case a REJECT is sent.
    pub fn receive_accept(&mut self, accept: Accept) -> Result<Accepted, Reject> {
        self.proposer.observe_ballot(accept.1);
        self.acceptor.receive_accept(accept)
    }

    /// Handles ACCEPTED messages from acceptors.
    pub fn receive_accepted(&mut self, accepted: Accepted) -> Option<Resolution> {
        self.proposer.observe_ballot(accepted.1);
        self.learner.receive_accepted(accepted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposer_propose_value() {
        // propose value with empty producer (hasn't started Phase 1)
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: None,
            current: 1,
            quorum: 2,
        };

        let prepare = proposer.propose_value(vec![0x0u8, 0xffu8]);
        assert_matches!(
            prepare,
            Some(ProposerMsg::Prepare(Prepare(1, Ballot(0, 1))))
        );
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                ref promises,
                ref promise_rejections,
                ..
            } if promises.is_empty() && promise_rejections.is_empty()
        );

        // now propose another value (should be ignored)
        let prepare = proposer.propose_value(vec![0xeeu8, 0xeeu8]);
        assert_matches!(prepare, None);
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                value: Some(ref v),
                ..
            } if v == &vec![0x0u8, 0xffu8]
        );

        // propose value with producer that has started phase 1 (but no value has been proposed)
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: None,
            current: 1,
            quorum: 2,
        };

        proposer.prepare(None);
        proposer.propose_value(vec![0xfeu8]);

        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                value: Some(ref v),
                ..
            } if v == &vec![0xfeu8]
        );

        // propose value with producer that has completed Phase 1
        // but has not yet started Phase 2
        let mut proposer = Proposer {
            state: ProposerState::Leader {
                proposal: Ballot(0, 1),
                value: None,
                rejections: QuorumSet::with_size(2),
            },
            highest: None,
            current: 1,
            quorum: 2,
        };

        let accept = proposer.propose_value(vec![0x22u8]);
        assert_matches!(
            accept,
            Some(ProposerMsg::Accept(Accept(1, Ballot(0, 1), ref v))) if v == &vec![0x22u8]
        );

        assert_matches!(
            proposer.state,
            ProposerState::Leader {
                proposal: Ballot(0, 1),
                value: Some(ref v),
                ..
            } if v == &vec![0x22u8]
        );

        // now try to propose an alternate value, which should be rejected
        let res = proposer.propose_value(vec![0x33u8]);
        assert_matches!(res, None);
        assert_matches!(
            proposer.state,
            ProposerState::Leader {
                proposal: Ballot(0, 1),
                value: Some(ref v),
                ..
            } if v == &vec![0x22u8]
        );
    }

    #[test]
    fn test_proposer_prepare() {
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 2,
        };

        let prepare = proposer.prepare(None);
        assert_matches!(prepare, Prepare(1, Ballot(101, 1)));
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: None,
                ..
            }
        );
    }

    #[test]
    fn test_proposer_receive_promise() {
        // start a producer that receives a quorum with no accepted valaues
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 2,
        };

        proposer.prepare(None);

        let accept = proposer.receive_promise(Promise(0, Ballot(101, 1), None));
        assert!(accept.is_none());
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: None,
                ..
            }
        );

        let accept = proposer.receive_promise(Promise(2, Ballot(101, 1), None));
        assert!(accept.is_none());
        assert_matches!(
            proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                value: None,
                ..
            }
        );

        // start a producer with proposed value that receives a quorum with no accepted value
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 2,
        };

        proposer.propose_value(vec![0x1u8]);
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: Some(_),
                ..
            }
        );

        let accept = proposer.receive_promise(Promise(0, Ballot(101, 1), None));
        assert!(accept.is_none());

        let accept = proposer.receive_promise(Promise(2, Ballot(101, 1), None));
        assert_matches!(
            accept,
            Some(Accept(1, Ballot(101, 1), ref v)) if v == &vec![0x1u8]
        );

        // start a producer with proposed value that receives a quorum with and accepted value
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 3,
        };

        proposer.propose_value(vec![0x1u8]);
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: Some(_),
                ..
            }
        );

        let accept = proposer.receive_promise(Promise(
            3,
            Ballot(101, 1),
            Some((Ballot(90, 0), vec![0x4u8])),
        ));
        assert!(accept.is_none());
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: Some(Ballot(90, 0)),
                value: Some(_),
                ..
            }
        );

        let accept = proposer.receive_promise(Promise(
            0,
            Ballot(101, 1),
            Some((Ballot(100, 0), vec![0x8u8])),
        ));
        assert!(accept.is_none());
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: Some(Ballot(100, 0)),
                value: Some(_),
                ..
            }
        );

        let accept = proposer.receive_promise(Promise(
            2,
            Ballot(101, 1),
            Some((Ballot(99, 0), vec![0x9u8])),
        ));
        assert_matches!(
            accept,
            Some(Accept(1, Ballot(101, 1), ref v)) if v == &vec![0x8u8]
        );
        assert_matches!(
            proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                value: Some(ref v),
                ..
            } if v == &vec![0x8u8]
        );
    }

    #[test]
    fn proposer_receive_reject() {
        // start a producer that receives rejections during Phase 1
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 2,
        };

        proposer.prepare(None);

        let new_prepare = proposer.receive_reject(Reject(3, Ballot(101, 1), Ballot(102, 2)));
        assert!(new_prepare.is_none());
        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ref promise_rejections,
                ..
            } if promise_rejections.contains(3));

        // test ballot != proposed (out of order)
        let new_prepare = proposer.receive_reject(Reject(2, Ballot(99, 1), Ballot(102, 2)));
        assert!(new_prepare.is_none());

        let new_prepare = proposer.receive_reject(Reject(4, Ballot(101, 1), Ballot(102, 2)));
        assert!(new_prepare.is_some());
        assert_matches!(new_prepare.unwrap(), Prepare(1, Ballot(103, 1)));

        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(103, 1),
                ref promise_rejections,
                ..
            } if promise_rejections.is_empty());

        // start a producer that receives rejections during Phase 1 and Phase 2
        let mut proposer = Proposer {
            state: ProposerState::Empty,
            highest: Some(Ballot(100, 1)),
            current: 1,
            quorum: 2,
        };

        proposer.prepare(None);
        let accept = proposer.receive_promise(Promise(0, Ballot(101, 1), None));
        assert!(accept.is_none());

        let msg = proposer.receive_reject(Reject(2, Ballot(101, 1), Ballot(103, 5)));
        assert!(msg.is_none());

        let msg = proposer.receive_promise(Promise(3, Ballot(101, 1), None));
        assert!(msg.is_none());

        assert_matches!(
            proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                ref rejections,
                ..
            } if !rejections.is_empty());

        // proposer lost leader status
        let msg = proposer.receive_reject(Reject(4, Ballot(101, 1), Ballot(103, 5)));
        assert!(msg.is_some());
        assert_matches!(msg.unwrap(), Prepare(1, Ballot(104, 1)));

        assert_matches!(
            proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(104, 1),

                // TODO: is this correct?
                value: None,
                ref promises,
                ref promise_rejections,
                highest_accepted: None
            } if promises.is_empty() && promise_rejections.is_empty());
    }

    #[test]
    fn acceptor_receive_prepare() {
        let mut acceptor = Acceptor {
            current: 3,
            promised: None,
            accepted: None,
        };

        // acceptor promises the ballot when nothing promised
        let res = acceptor.receive_prepare(Prepare(1, Ballot(100, 1)));
        assert!(res.is_ok());
        assert_matches!(res.unwrap(), Promise(3, Ballot(100, 1), None));
        assert_matches!(acceptor.promised, Some(Ballot(100, 1)));

        // acceptor promises higher ballots
        let res = acceptor.receive_prepare(Prepare(2, Ballot(102, 2)));
        assert!(res.is_ok());
        assert_matches!(res.unwrap(), Promise(3, Ballot(102, 2), None));
        assert_matches!(acceptor.promised, Some(Ballot(102, 2)));

        // acceptor will reject ballots < promised
        let res = acceptor.receive_prepare(Prepare(1, Ballot(101, 1)));
        assert!(res.is_err());
        assert_matches!(res.unwrap_err(), Reject(3, Ballot(101, 1), Ballot(102, 2)));
        assert_matches!(acceptor.promised, Some(Ballot(102, 2)));

        // prepare contains last accepted values
        acceptor.accepted = Some((Ballot(102, 2), vec![0x0, 0x1, 0x2]));
        let res = acceptor.receive_prepare(Prepare(1, Ballot(103, 1)));
        assert!(res.is_ok());
        assert_matches!(
            res.unwrap(),
            Promise(3, Ballot(103, 1), Some((Ballot(102, 2), ref v)))
            if v == &vec![0x0, 0x1, 0x2]
        );
        assert_matches!(acceptor.promised, Some(Ballot(103, 1)));
    }

    #[test]
    fn acceptor_receive_accept() {
        let mut acceptor = Acceptor {
            current: 2,
            promised: None,
            accepted: None,
        };

        // acceptor allows ACCEPT without a promise
        let res = acceptor.receive_accept(Accept(1, Ballot(101, 1), vec![0xee, 0xe0]));
        assert!(res.is_ok());
        assert_matches!(
            res.unwrap(),
            Accepted(2, Ballot(101, 1), ref v)
            if v == &vec![0xee, 0xe0]
        );
        assert_matches!(acceptor.promised, Some(Ballot(101, 1)));

        // acceptor sends REJECT with ballot less than promised OR accepted
        let res = acceptor.receive_accept(Accept(3, Ballot(100, 3), vec![0x0]));
        assert!(res.is_err());
        assert_matches!(res.unwrap_err(), Reject(2, Ballot(100, 3), Ballot(101, 1)));
    }

    #[test]
    fn learner_receive_accepted() {
        let mut learner = Learner {
            state: LearnerState::AwaitQuorum {
                proposals: HashMap::new(),
                acceptors: HashMap::new(),
            },
            current: 5,
            quorum: 2,
        };

        // accepts new ballots
        let val = vec![0x0u8, 0xeeu8];
        let resolution = learner.receive_accepted(Accepted(1, Ballot(100, 0), val.clone()));
        assert!(resolution.is_none());
        assert_matches!(
            learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ref proposals,
            } if acceptors.contains_key(&1) && proposals.contains_key(&Ballot(100, 0))
        );

        // ignores previous ballots from the same acceptor
        let resolution = learner.receive_accepted(Accepted(1, Ballot(90, 0), val.clone()));
        assert!(resolution.is_none());
        assert_matches!(
            learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ref proposals,
            }
            if *acceptors.get(&1).unwrap() == Ballot(100, 0) &&
               !proposals.contains_key(&Ballot(90, 0))
        );

        // allows quorum to be reached
        let resolution = learner.receive_accepted(Accepted(2, Ballot(100, 0), val.clone()));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(5, Ballot(100, 0), ref v)
            if v == &val
        );

        // ignores other ballots once quorum reached
        let resolution = learner.receive_accepted(Accepted(3, Ballot(90, 0), vec![0x0]));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(5, Ballot(100, 0), ref v)
            if v == &val
        );

        // adds acceptors that match the ballot
        let resolution = learner.receive_accepted(Accepted(4, Ballot(100, 0), val.clone()));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(5, Ballot(100, 0), ref v)
            if v == &val
        );
        assert_matches!(
            learner.state,
            LearnerState::Final {
                accepted: Ballot(100, 0),
                ref acceptors,
                ..
            }
            if acceptors.contains(4)
        );
    }
}
