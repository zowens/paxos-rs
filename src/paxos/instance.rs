use super::messages::*;
use super::*;
use crate::config::NodeId;
use bytes::Bytes;
use either::Either;
use std::collections::HashMap;
use std::mem;

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
        value: Option<Bytes>,
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
        value: Option<Bytes>,
        /// Rejections received from acceptors
        rejections: QuorumSet,
    },
}

impl ProposerState {
    fn take_value(&mut self) -> Option<Bytes> {
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

    /// Regenerates a PREPARE message with a new ballot
    fn force_prepare(&mut self) -> Prepare {
        let val = self.state.take_value();
        self.prepare(val)
    }

    /// Prepare sets state to candidate and begins to track promises.
    fn prepare(&mut self, value: Option<Bytes>) -> Prepare {
        let new_ballot = self
            .highest
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

        Prepare(new_ballot)
    }

    /// Revokes leadership status
    fn revoke_leadership(&mut self) {
        if let ProposerState::Leader { .. } = self.state {
            info!("Revoking Phase 1 quorum from Proposer");
            self.state = ProposerState::Empty;
        }
    }

    /// Proposes a value once Phase 1 is completed. It is not guaranteed that the value will
    /// be accepted, as the maximum accepted value from acceptors is used as preference.
    ///
    /// TODO: Do we need to note if the value is not used?
    fn propose_value(&mut self, v: Bytes) -> Option<ProposerMsg> {
        match self.state {
            // if the proposer is in the empty state, we'll attempt to run Phase 1
            // with a prepare message
            ProposerState::Empty => Some(Either::Left(self.prepare(Some(v)))),
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
            } if value.is_none() => {
                *value = Some(v.clone());
                Some(Either::Right(Accept(proposal, v)))
            }
            _ => None,
        }
    }

    /// Handler for REJECT from an acceptor peer. Phase 1 with a higher ballot is returned
    /// if the rejection has quorum.
    fn receive_reject(&mut self, peer: NodeId, reject: Reject) -> Option<Prepare> {
        let Reject(proposed, promised) = reject;
        debug!(
            "Received REJECT for {:?} with greater {:?} from peer {}",
            proposed, promised, peer
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
            } if proposal == proposed => {
                promise_rejections.insert(peer);
                promise_rejections.has_quorum()
            }
            ProposerState::Leader {
                ref mut rejections,
                proposal,
                ..
            } if proposal == proposed => {
                rejections.insert(peer);
                rejections.has_quorum()
            }
            _ => false,
        };

        if lost_leadership {
            debug!("Lost leadership, restarting prepare");
            Some(self.force_prepare())
        } else {
            None
        }
    }

    /// Note a promise from a peer. An ACCEPT message is returned if quorum is detected.
    fn receive_promise(&mut self, peer: NodeId, promise: Promise) -> Option<Accept> {
        let Promise(proposed, accepted) = promise;
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
                if let Some(PromiseValue(bal, v)) = accepted {
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
                let accept = value.clone().map(|v| Accept(proposal, v));

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
    /// last promised ballot within this instance
    promised: Option<Ballot>,
    /// last accepted ballot/value pair within this instance
    accepted: Option<PromiseValue>,
}

impl Acceptor {
    /// Handler for a PREPARE message sent from a proposer. The result is either a PROMISE
    /// to the proposer to not accept ballots > proposal or a REJECT if a ballot has been
    /// promised with a ballot > proposal.
    fn receive_prepare(
        &mut self,
        peer: NodeId,
        prepare: Prepare,
    ) -> Either<Reply<Promise>, Reply<Reject>> {
        let Prepare(proposal) = prepare;

        debug_assert_eq!(
            peer, proposal.1,
            "Proposal should be from the same numbered peer"
        );

        let opposing_ballot = self.promised.filter(|b| *b > proposal);

        match opposing_ballot {
            Some(b) => {
                debug!("Rejecting proposed {:?} with greater {:?}", proposal, b);
                Either::Right(Reject(proposal, b).reply_to(peer))
            }
            None => {
                debug!("Promising {:?}", proposal);

                // track the proposal as the highest promise
                // (in order to reject ballots < proposal)
                self.promised = Some(proposal);
                Either::Left(Promise(proposal, self.accepted.clone()).reply_to(peer))
            }
        }
    }

    /// Handler for an ACCEPT message, which is sent from a proposer when a quorum
    /// for the Phase 1 PREPARE has been made from acceptors. Opposing ballots may still
    /// happen in Phase 2, in which case a REJECT is sent.
    fn receive_accept(&mut self, peer: NodeId, accept: Accept) -> Either<Accepted, Reply<Reject>> {
        let Accept(proposal, value) = accept;
        let opposing_ballot = self.promised.filter(|b| b > &proposal);
        match opposing_ballot {
            Some(b) => {
                debug!(
                    "Rejecting ACCEPT message with ballot {:?} because of greater {:?}",
                    proposal, b
                );
                Either::Right(Reject(proposal, b).reply_to(peer))
            }
            None => {
                debug!("Accepting proposal {:?} from peer={}", proposal, peer);

                // set the accepted value, which is sent to subsequent PROMISE responses
                // with ballts greater than the current proposal
                self.accepted = Some(PromiseValue(proposal, value.clone()));

                // set the promised value accordingly. In Paxos, it is possible
                // for an acceptor to miss the PREPARE (as in, not participate in quorum)
                // yet still participate in Phase 2 quorum. Once this is the case, we need
                // to ensure that future PREPARE messages from Proposers will not be lower
                // than the accepted value from this ACCEPT message.
                self.promised = Some(proposal);
                Either::Left(Accepted(proposal))
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
    value: Bytes,
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
        value: Bytes,
    },
}

/// Handler for state transitions for the Learner role. A Paxos Learner listens
/// for ACCEPTED messages in order to determine quorum for the final value.
struct Learner {
    /// state of the learner (AwaitQuorum or Final)
    state: LearnerState,
    /// Size of quorum
    quorum: usize,
}

impl Learner {
    /// Notice (ballot, value) pairs
    fn notice_value(&mut self, bal: Ballot, value: Bytes) {
        if let LearnerState::AwaitQuorum {
            ref mut proposals, ..
        } = self.state
        {
            let quorum = self.quorum;
            proposals.entry(bal).or_insert_with(|| ProposalStatus {
                acceptors: QuorumSet::with_size(quorum),
                value,
            });
        }
    }

    /// Handles ACCEPTED messages from acceptors.
    fn receive_accepted(&mut self, peer: NodeId, accepted: Accepted) -> Option<Resolution> {
        let Accepted(proposal) = accepted;

        let resolve_value = match self.state {
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
                    Vacant(e) => {
                        e.insert(proposal);
                    }
                }

                // insert the ACCEPTED as part of the ballot
                debug!("Accepted {:?} for peer={}", proposal, peer);
                match proposals.entry(proposal) {
                    Occupied(ref mut entry) => {
                        let proposal_status = entry.get_mut();
                        proposal_status.acceptors.insert(peer);
                        // if learner has has quorum of ACCEPTED messages, transition to final
                        // otherwise no resolution has been reached
                        if !proposal_status.acceptors.has_quorum() {
                            return None;
                        }
                        proposal_status.value.clone()
                    }
                    Vacant(_) => {
                        warn!("Unknown value for ballot {:?}", proposal);
                        return None;
                    }
                }
            }
            LearnerState::Final {
                accepted,
                ref value,
            } => {
                return Some(Resolution(accepted, value.clone()));
            }
        };

        debug!("Quorum reached for Phase 2 {:?}", proposal);

        // a final value has been selected, move the state to final
        self.state = LearnerState::Final {
            value: resolve_value.clone(),
            accepted: proposal,
        };

        Some(Resolution(proposal, resolve_value))
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
    pub fn new(
        current: NodeId,
        quorum: usize,
        promised: Option<Ballot>,
        accepted: Option<PromiseValue>,
    ) -> PaxosInstance {
        let mut learner = Learner {
            state: LearnerState::AwaitQuorum {
                proposals: HashMap::with_capacity(quorum * 2),
                acceptors: HashMap::with_capacity(quorum * 2),
            },
            quorum,
        };
        if let Some(PromiseValue(bal, val)) = accepted.clone() {
            learner.notice_value(bal, val);
        }

        PaxosInstance {
            proposer: Proposer {
                state: ProposerState::Empty,
                highest: promised,
                current,
                quorum,
            },
            acceptor: Acceptor { promised, accepted },
            learner,
        }
    }

    /// Creates an instance for which the current node is the leader.
    pub fn with_leadership(
        current: NodeId,
        quorum: usize,
        previous_ballot: Ballot,
    ) -> PaxosInstance {
        // TODO: if `accepted` is Some, add to learner

        PaxosInstance {
            proposer: Proposer {
                state: ProposerState::Leader {
                    proposal: previous_ballot,
                    value: None,
                    rejections: QuorumSet::with_size(quorum),
                },
                highest: Some(previous_ballot),
                current,
                quorum,
            },
            acceptor: Acceptor {
                promised: Some(previous_ballot),
                accepted: None,
            },
            learner: Learner {
                state: LearnerState::AwaitQuorum {
                    proposals: HashMap::with_capacity(quorum * 2),
                    acceptors: HashMap::with_capacity(quorum * 2),
                },
                quorum,
            },
        }
    }

    /// Starts Phase 1 with a possibly new PREPARE message.
    pub fn prepare(&mut self) -> Prepare {
        let prepare = self.proposer.force_prepare();

        // node auto-accepts proposal from itself
        let current_node = self.proposer.current;
        match self.acceptor.receive_prepare(current_node, prepare.clone()) {
            Either::Left(Reply { message, .. }) => {
                self.proposer.receive_promise(current_node, message);
            }
            Either::Right(reject) => {
                panic!("Generated proposal the node could not accept {:?}", reject)
            }
        }

        prepare
    }

    /// Proposes a value once Phase 1 is completed. It is not guaranteed that the value will
    /// be accepted, as the maximum accepted value from acceptors is used as preference.
    ///
    /// TODO: Do we need to note if the value is not used?
    pub fn propose_value(&mut self, v: Bytes) -> Option<ProposerMsg> {
        match self.proposer.propose_value(v.clone()) {
            Some(Either::Left(prepare)) => {
                self.learner.notice_value(prepare.0, v);

                // node auto-accepts proposal from itself
                let current_node = self.proposer.current;
                match self.acceptor.receive_prepare(current_node, prepare.clone()) {
                    Either::Left(Reply { message, .. }) => {
                        self.proposer.receive_promise(current_node, message);
                    }
                    Either::Right(reject) => {
                        panic!("Generated proposal the node could not accept {:?}", reject)
                    }
                }
                Some(Either::Left(prepare))
            }
            Some(Either::Right(accept)) => {
                self.learner.notice_value(accept.0, v);

                // track the proposer as accepting the ballot
                self.learner
                    .receive_accepted(self.proposer.current, Accepted(accept.0));
                Some(Either::Right(accept))
            }
            v => v,
        }
    }

    /// Revokes Phase 1 quorum from the proposer when a master lease has expired.
    pub fn revoke_leadership(&mut self) {
        self.proposer.revoke_leadership();
    }

    /// Handler for a PROMISE from a peer. An ACCEPT message is returned if quorum is detected.
    pub fn receive_promise(&mut self, peer: NodeId, promise: Promise) -> Option<Accept> {
        match self.proposer.receive_promise(peer, promise) {
            Some(accept) => {
                // track the proposer as accepting the ballot
                self.learner
                    .receive_accepted(self.proposer.current, Accepted(accept.0));
                Some(accept)
            }
            None => None,
        }
    }

    /// Handler for REJECT from an acceptor peer. Phase 1 with a higher ballot is returned
    /// if the rejection has quorum.
    pub fn receive_reject(&mut self, peer: NodeId, reject: Reject) -> Option<Prepare> {
        self.proposer.receive_reject(peer, reject)
    }

    /// Handler for a PREPARE message sent from a proposer. The result is either a PROMISE
    /// to the proposer to not accept ballots > proposal or a REJECT if a ballot has been
    /// promised with a ballot > proposal.
    pub fn receive_prepare(
        &mut self,
        peer: NodeId,
        prepare: Prepare,
    ) -> Either<Reply<Promise>, Reply<Reject>> {
        self.proposer.observe_ballot(prepare.0);
        self.acceptor.receive_prepare(peer, prepare)
    }

    /// Handler for an ACCEPT message, which is sent from a proposer when a quorum
    /// for the Phase 1 PREPARE has been made from acceptors. Opposing ballots may still
    /// happen in Phase 2, in which case a REJECT is sent.
    pub fn receive_accept(
        &mut self,
        peer: NodeId,
        accept: Accept,
    ) -> Either<(Accepted, Option<Resolution>), Reply<Reject>> {
        self.proposer.observe_ballot(accept.0);
        self.learner.notice_value(accept.0, accept.1.clone());
        match self.acceptor.receive_accept(peer, accept) {
            Either::Left(accepted) => {
                // track self as accepting in the learner state machine
                // (without propagating the message to the network)
                let resolution = self
                    .learner
                    .receive_accepted(self.proposer.current, accepted.clone());
                debug_assert!(
                    resolution.is_none(),
                    "Should not have resolved with a single vote"
                );

                // track sender as accepting as well
                let resolution = self.learner.receive_accepted(peer, accepted.clone());
                Either::Left((accepted, resolution))
            }
            Either::Right(v) => Either::Right(v),
        }
    }

    /// Handles ACCEPTED messages from acceptors.
    pub fn receive_accepted(&mut self, peer: NodeId, accepted: Accepted) -> Option<Resolution> {
        self.proposer.observe_ballot(accepted.0);
        self.learner.receive_accepted(peer, accepted)
    }

    /// Ballot that was last promised to a proposer
    pub fn last_promised(&self) -> Option<Ballot> {
        self.acceptor.promised
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_value() {
        // propose value with empty producer (hasn't started Phase 1)
        let mut paxos = PaxosInstance::new(1, 2, None, None);

        let prepare = paxos.propose_value(vec![0x0u8, 0xffu8].into());
        assert_matches!(prepare, Some(Either::Left(Prepare(Ballot(0, 1)))));
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                ref promises,
                ref promise_rejections,
                ..
            } if promises.contains(1) && promise_rejections.is_empty()
        );

        // now propose another value (should be ignored)
        let prepare = paxos.propose_value(vec![0xeeu8, 0xeeu8].into());
        assert_matches!(prepare, None);

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                value: Some(v),
                ..
            } if v == Bytes::from(vec![0x0u8, 0xffu8])
        );

        // propose value with producer that has started phase 1 (but no value has been proposed)
        let mut paxos = PaxosInstance::new(1, 2, None, None);

        paxos.prepare();
        paxos.propose_value(vec![0xfeu8].into());

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(0, 1),
                highest_accepted: None,
                value: Some(v),
                ..
            } if v == Bytes::from(vec![0xfeu8])
        );

        // propose value with producer that has completed Phase 1
        // but has not yet started Phase 2
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        paxos.proposer.state = ProposerState::Leader {
            proposal: Ballot(0, 1),
            value: None,
            rejections: QuorumSet::with_size(2),
        };

        let accept = paxos.propose_value(vec![0x22u8].into());
        assert_matches!(
            accept,
            Some(Either::Right(Accept(Ballot(0, 1), v))) if v == Bytes::from(vec![0x22u8])
        );

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Leader {
                proposal: Ballot(0, 1),
                value: Some(ref v),
                ..
            } if v.clone() == Bytes::from(vec![0x22u8])
        );

        // assert that the ACCEPT is tracked in the learner state
        assert_matches!(
            paxos.learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ..
            } if acceptors.get(&1).cloned() == Some(Ballot(0, 1))
        );

        // now try to propose an alternate value, which should be rejected
        let res = paxos.propose_value(vec![0x33u8].into());
        assert_matches!(res, None);
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Leader {
                proposal: Ballot(0, 1),
                value: Some(v),
                ..
            } if v == Bytes::from(vec![0x22u8])
        );
    }

    #[test]
    fn prepare() {
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        let prepare = paxos.prepare();
        assert_matches!(prepare, Prepare(Ballot(101, 1)));
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: None,
                ..
            }
        );
    }

    #[test]
    fn receive_promise() {
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        paxos.prepare();

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: None,
                ref promises,
                ..
            } if promises.contains(1)
        );

        let accept = paxos.receive_promise(2, Promise(Ballot(101, 1), None));
        assert!(accept.is_none());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                value: None,
                ..
            }
        );

        // start a producer with proposed value that receives a quorum with no accepted value
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        paxos.propose_value(vec![0x1u8].into());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: Some(_),
                ..
            }
        );

        let accept = paxos.receive_promise(2, Promise(Ballot(101, 1), None));
        assert_matches!(
            accept,
            Some(Accept(Ballot(101, 1), v)) if v == Bytes::from(vec![0x1u8])
        );

        // assert that the ACCEPT is tracked in the learner state
        assert_matches!(
            paxos.learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ..
            } if acceptors.get(&1).cloned() == Some(Ballot(101, 1))
        );

        // start a producer with proposed value that receives a quorum with an accepted value
        let mut paxos = PaxosInstance::new(1, 3, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        paxos.proposer.propose_value(vec![0x1u8].into());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: None,
                value: Some(_),
                ..
            }
        );

        let accept = paxos.receive_promise(
            3,
            Promise(
                Ballot(101, 1),
                Some(PromiseValue(Ballot(90, 0), vec![0x4u8].into())),
            ),
        );
        assert!(accept.is_none());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: Some(Ballot(90, 0)),
                value: Some(_),
                ..
            }
        );

        let accept = paxos.receive_promise(
            0,
            Promise(
                Ballot(101, 1),
                Some(PromiseValue(Ballot(100, 0), vec![0x8u8].into())),
            ),
        );
        assert!(accept.is_none());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                highest_accepted: Some(Ballot(100, 0)),
                value: Some(_),
                ..
            }
        );

        let accept = paxos.receive_promise(
            2,
            Promise(
                Ballot(101, 1),
                Some(PromiseValue(Ballot(99, 0), vec![0x9u8].into())),
            ),
        );
        assert_matches!(
            accept,
            Some(Accept(Ballot(101, 1), v)) if v == Bytes::from(vec![0x8u8])
        );
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                value: Some(v),
                ..
            } if v == Bytes::from(vec![0x8u8])
        );
    }

    #[test]
    fn receive_reject() {
        // start a producer that receives rejections during Phase 1
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        paxos.prepare();

        let new_prepare = paxos.receive_reject(3, Reject(Ballot(101, 1), Ballot(102, 2)));
        assert!(new_prepare.is_none());
        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ref promise_rejections,
                ..
            } if promise_rejections.contains(3));

        // test ballot != proposed (out of order)
        let new_prepare = paxos.receive_reject(2, Reject(Ballot(99, 1), Ballot(102, 2)));
        assert!(new_prepare.is_none());

        let new_prepare = paxos.receive_reject(4, Reject(Ballot(101, 1), Ballot(102, 2)));
        assert!(new_prepare.is_some());
        assert_matches!(new_prepare.unwrap(), Prepare(Ballot(103, 1)));

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Candidate {
                proposal: Ballot(103, 1),
                ref promise_rejections,
                ..
            } if promise_rejections.is_empty());

        // start a producer that receives rejections during Phase 1 and Phase 2
        let mut paxos = PaxosInstance::new(1, 2, None, None);
        // fake observing high ballot
        paxos.proposer.observe_ballot(Ballot(100, 1));

        paxos.prepare();
        let accept = paxos.receive_promise(0, Promise(Ballot(101, 1), None));
        assert!(accept.is_none());

        let msg = paxos.receive_reject(2, Reject(Ballot(101, 1), Ballot(103, 5)));
        assert!(msg.is_none());

        let msg = paxos.receive_promise(3, Promise(Ballot(101, 1), None));
        assert!(msg.is_none());

        assert_matches!(
            paxos.proposer.state,
            ProposerState::Leader {
                proposal: Ballot(101, 1),
                ref rejections,
                ..
            } if !rejections.is_empty());

        // proposer lost leader status
        let msg = paxos.receive_reject(4, Reject(Ballot(101, 1), Ballot(103, 5)));
        assert!(msg.is_some());
        assert_matches!(msg.unwrap(), Prepare(Ballot(104, 1)));

        assert_matches!(
            paxos.proposer.state,
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
    fn receive_prepare() {
        let mut paxos = PaxosInstance::new(0, 2, None, None);

        // acceptor promises the ballot when nothing promised
        let res = paxos.receive_prepare(1, Prepare(Ballot(100, 1)));
        assert!(res.is_left());
        assert_eq!(
            res.left().unwrap(),
            Reply {
                reply_to: 1,
                message: Promise(Ballot(100, 1), None),
            }
        );
        assert_eq!(paxos.acceptor.promised, Some(Ballot(100, 1)));

        // acceptor promises higher ballots
        let res = paxos.receive_prepare(2, Prepare(Ballot(102, 2)));
        assert!(res.is_left());
        assert_eq!(
            res.left().unwrap(),
            Reply {
                reply_to: 2,
                message: Promise(Ballot(102, 2), None),
            }
        );
        assert_eq!(paxos.acceptor.promised, Some(Ballot(102, 2)));

        // acceptor will reject ballots < promised
        let res = paxos.receive_prepare(1, Prepare(Ballot(101, 1)));
        assert!(res.is_right());
        assert_eq!(
            res.right().unwrap(),
            Reply {
                reply_to: 1,
                message: Reject(Ballot(101, 1), Ballot(102, 2)),
            }
        );
        assert_eq!(paxos.acceptor.promised, Some(Ballot(102, 2)));

        // prepare contains last accepted values
        paxos.acceptor.accepted = Some(PromiseValue(Ballot(102, 2), vec![0x0, 0x1, 0x2].into()));
        let res = paxos.receive_prepare(1, Prepare(Ballot(103, 1)));
        assert!(res.is_left());
        let expected_value = vec![0x0, 0x1, 0x2].into();
        assert_eq!(
            res.left().unwrap(),
            Reply {
                reply_to: 1,
                message: Promise(
                    Ballot(103, 1),
                    Some(PromiseValue(Ballot(102, 2), expected_value))
                ),
            }
        );
        assert_matches!(paxos.acceptor.promised, Some(Ballot(103, 1)));
    }

    #[test]
    fn receive_accept() {
        let mut paxos = PaxosInstance::new(0, 3, None, None);

        // acceptor allows ACCEPT without a promise
        let res = paxos.receive_accept(1, Accept(Ballot(101, 1), vec![0xee, 0xe0].into()));
        assert!(res.is_left());
        assert_eq!(res.left().unwrap(), (Accepted(Ballot(101, 1)), None));
        assert_matches!(paxos.acceptor.promised, Some(Ballot(101, 1)));

        // acceptor sends REJECT with ballot less than promised OR accepted
        let res = paxos.receive_accept(3, Accept(Ballot(100, 3), vec![0x0].into()));
        assert!(res.is_right());
        assert_eq!(
            res.right().unwrap(),
            Reply {
                reply_to: 3,
                message: Reject(Ballot(100, 3), Ballot(101, 1)),
            }
        );

        // acceptor allows ACCEPT to quorum
        let res = paxos.receive_accept(2, Accept(Ballot(101, 1), vec![0xee, 0xe0].into()));
        assert!(res.is_left());
        assert_eq!(
            res.left().unwrap(),
            (
                Accepted(Ballot(101, 1)),
                Some(Resolution(Ballot(101, 1), vec![0xee, 0xe0].into()))
            )
        );
        assert_matches!(paxos.acceptor.promised, Some(Ballot(101, 1)));
    }

    #[test]
    fn receive_accepted() {
        let mut paxos = PaxosInstance::new(0, 2, None, None);

        // accepts new ballots
        let val: Bytes = vec![0x0u8, 0xeeu8].into();
        paxos.learner.notice_value(Ballot(100, 0), val.clone());
        let resolution = paxos.receive_accepted(1, Accepted(Ballot(100, 0)));
        assert!(resolution.is_none());
        assert_matches!(
            paxos.learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ref proposals,
            } if acceptors.contains_key(&1) && proposals.contains_key(&Ballot(100, 0))
        );

        // ignores previous ballots from the same acceptor
        let resolution = paxos.receive_accepted(1, Accepted(Ballot(90, 0)));
        assert!(resolution.is_none());
        assert_matches!(
            paxos.learner.state,
            LearnerState::AwaitQuorum {
                ref acceptors,
                ref proposals,
            }
            if *acceptors.get(&1).unwrap() == Ballot(100, 0) &&
               !proposals.contains_key(&Ballot(90, 0))
        );

        // allows quorum to be reached
        let resolution = paxos.receive_accepted(2, Accepted(Ballot(100, 0)));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(Ballot(100, 0), ref v)
            if v == &val
        );

        // ignores other ballots once quorum reached
        let resolution = paxos.receive_accepted(3, Accepted(Ballot(90, 0)));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(Ballot(100, 0), ref v)
            if v == &val
        );

        // adds acceptors that match the ballot
        let resolution = paxos.receive_accepted(4, Accepted(Ballot(100, 0)));
        assert!(resolution.is_some());
        assert_matches!(
            resolution.unwrap(),
            Resolution(Ballot(100, 0), ref v)
            if v == &val
        );
        assert_matches!(
            paxos.learner.state,
            LearnerState::Final {
                accepted: Ballot(100, 0),
                ..
            }
        );
    }
}
