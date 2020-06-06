use crate::{config::QuorumSet, Ballot, NodeId};
use bytes::Bytes;
use std::cmp::max;

/// Encoding of Acceptor (persistent Paxos memory) role
pub struct Acceptor {
    /// last accepted ballot/value pair within this instance
    state: AcceptorState,
}

impl Acceptor {
    /// New acceptor with last promised ballot and Phase 2 quorum size
    pub fn new(promised: Option<Ballot>, quorum: usize) -> Acceptor {
        assert!(quorum > 1);
        Acceptor {
            state: AcceptorState::AwaitValue { promised, quorum },
        }
    }

    /// Ballot of the last promise
    pub fn promised(&self) -> Option<Ballot> {
        match self.state {
            AcceptorState::AwaitValue { promised, .. } => promised,
            AcceptorState::AwaitQuorum { promised, .. } => Some(promised),
            AcceptorState::Resolved { accepted, .. } => Some(accepted),
        }
    }

    /// Value of the highest accepted value
    pub fn highest_value(&self) -> Option<(Ballot, Bytes)> {
        match self.state {
            AcceptorState::AwaitValue { .. } => None,
            AcceptorState::AwaitQuorum { ref proposed, .. } => Some(proposed.clone()),
            AcceptorState::Resolved {
                accepted,
                ref value,
            } => Some((accepted, value.clone())),
        }
    }

    /// Shows the resolution, if available
    pub fn resolution(&self) -> Option<(Ballot, Bytes)> {
        if let AcceptorState::Resolved {
            accepted,
            ref value,
        } = self.state
        {
            Some((accepted, value.clone()))
        } else {
            None
        }
    }

    /// Indicator of the learner considering the instance resolved to a value
    pub fn resolved(&self) -> bool {
        if let AcceptorState::Resolved { .. } = self.state {
            true
        } else {
            false
        }
    }

    /// Resolves a value within the learner state
    pub fn resolve(&mut self, bal: Ballot, val: Bytes) {
        // ignore if the acceptor is already resolved
        match self.state {
            AcceptorState::Resolved {
                accepted,
                ref value,
            } => {
                if accepted != bal || val != value {
                    warn!("Attempt to resolve to a different ballot or value. Accepted=<{:?},{:?}>, Attempted=<{:?},{:?}>",
                        accepted, value, bal, val);
                }
                return;
            }
            _ => {}
        }

        self.state = AcceptorState::Resolved {
            accepted: bal,
            value: val,
        };
    }

    /// Handler for a PREPARE message sent from a proposer. The result is either a PROMISE
    /// to the proposer to not accept ballots > proposal or a REJECT if a ballot has been
    /// promised with a ballot > proposal.
    pub fn receive_prepare(&mut self, ballot: Ballot) -> PrepareResponse {
        if self.resolved() {
            return PrepareResponse::Resolved;
        }

        match self.state {
            AcceptorState::AwaitValue {
                ref mut promised, ..
            } => match *promised {
                Some(promised_ballot) if promised_ballot > ballot => PrepareResponse::Reject {
                    proposed: ballot,
                    preempted: promised_ballot,
                },
                _ => {
                    *promised = Some(ballot);
                    PrepareResponse::Promise {
                        proposed: ballot,
                        value: None,
                    }
                }
            },
            AcceptorState::AwaitQuorum {
                ref mut promised,
                ref proposed,
                ..
            } => {
                if *promised > ballot {
                    PrepareResponse::Reject {
                        proposed: ballot,
                        preempted: *promised,
                    }
                } else {
                    *promised = ballot;
                    PrepareResponse::Promise {
                        proposed: ballot,
                        value: Some(proposed.clone()),
                    }
                }
            }
            AcceptorState::Resolved { .. } => PrepareResponse::Resolved,
        }
    }

    /// Handler for an ACCEPT message, which is sent from a proposer when a quorum
    /// for the Phase 1 PREPARE has been made from acceptors. Opposing ballots may still
    /// happen in Phase 2, in which case a REJECT is sent.
    pub fn receive_accept(&mut self, ballot: Ballot, value: Bytes) -> AcceptResponse {
        // set the promised value accordingly. In Paxos, it is possible
        // for an acceptor to miss the PREPARE (as in, not participate in quorum)
        // yet still participate in Phase 2 quorum. Once this is the case, we need
        // to ensure that future PREPARE messages from Proposers will not be lower
        // than the accepted value from this ACCEPT message.
        match self.state {
            AcceptorState::AwaitValue {
                ref mut promised, ..
            } => {
                if *promised > Some(ballot) {
                    return AcceptResponse::Reject {
                        preempted: promised.unwrap(),
                        proposed: ballot,
                    };
                }
                *promised = Some(ballot);
            }
            AcceptorState::AwaitQuorum {
                ref mut promised, ..
            } => {
                if *promised > ballot {
                    return AcceptResponse::Reject {
                        preempted: *promised,
                        proposed: ballot,
                    };
                }
                *promised = ballot;
            }
            AcceptorState::Resolved { .. } => return AcceptResponse::Resolved,
        };

        // Add the value iff the ballot is higher than previously accepted values
        let preempted_proposal = self.notice_value(ballot, value);

        AcceptResponse::Accepted {
            proposed: ballot,
            preempted_proposal,
        }
    }

    /// Updates the acceptor if the ballot and value are greater than
    /// the previously ACCEPT-ed ballot. This is used during Phase 1
    /// when the proposer receives previously accepted proposals.
    ///
    /// The value returned is the ballot and value that was previously the
    /// highest see by the acceptor.
    pub fn notice_value(&mut self, ballot: Ballot, value: Bytes) -> Option<(Ballot, Bytes)> {
        let (next_state, preempted_proposal) = match self.state {
            AcceptorState::AwaitValue { promised, quorum } => {
                // quorum must be at least _2_. We remove 1 from this state in order
                // to consider this node as accepting all Phase 2 proposals as both
                // the Distinguished Proposer and an Acceptor
                assert!(quorum > 1);
                (
                    Some(AcceptorState::AwaitQuorum {
                        promised: max(Some(ballot), promised).unwrap(),
                        proposed: (ballot, value),
                        quorum: QuorumSet::with_size(quorum - 1),
                    }),
                    None,
                )
            }
            AcceptorState::AwaitQuorum {
                promised,
                proposed: (bal, ref val),
                ref quorum,
            } if bal < ballot => (
                Some(AcceptorState::AwaitQuorum {
                    promised: max(promised, ballot),
                    proposed: (ballot, value),
                    quorum: QuorumSet::with_size(quorum.len()),
                }),
                Some((bal, val.clone())),
            ),
            _ => (None, None),
        };

        if let Some(next_state) = next_state {
            self.state = next_state;
        }

        preempted_proposal
    }

    /// Received ACCEPTED messages based on a proposal from this acceptor
    pub fn receive_accepted(&mut self, peer: NodeId, ballot: Ballot) {
        let resolution = match self.state {
            AcceptorState::AwaitQuorum {
                ref proposed,
                ref mut quorum,
                ..
            } if ballot == proposed.0 => {
                quorum.insert(peer);
                if quorum.has_quorum() {
                    Some(proposed.clone())
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some((accepted, value)) = resolution {
            self.state = AcceptorState::Resolved { accepted, value };
        }
    }
}

/// Result of receiving a PREPARE from a proposer
#[derive(Debug, PartialEq, Eq)]
pub enum PrepareResponse {
    /// Acceptor has promised to not accept a ballot less than the
    /// proposed ballot
    Promise {
        proposed: Ballot,
        value: Option<(Ballot, Bytes)>,
    },
    /// Phase 1 is rejected due to a previously accepted ballot
    /// that is higher than the proposed ballot
    Reject { proposed: Ballot, preempted: Ballot },
    /// Acceptor has previously resolved the value during Phase 3
    Resolved,
}

/// Result of receiving an ACCEPT from a proposer
#[derive(Debug, PartialEq, Eq)]
pub enum AcceptResponse {
    /// Acceptor has accepted the value of the proposed ballot
    Accepted {
        proposed: Ballot,
        /// Proposal that the acceptor as previously ACCEPTED
        /// with ballot < proposed
        preempted_proposal: Option<(Ballot, Bytes)>,
    },
    /// Phase 2 is rejected due to a previously accepted ballot
    /// that is higher than the accept message ballot
    Reject { proposed: Ballot, preempted: Ballot },
    /// Acceptor has previously resolved the value during Phase 3
    Resolved,
}

#[derive(Debug)]
enum AcceptorState {
    AwaitValue {
        /// last promised ballot within this instance
        promised: Option<Ballot>,

        /// Size of the Phase 2 quorum
        quorum: usize,
    },

    /// The acceptor has received an ACCEPT message from a proposer with a value
    AwaitQuorum {
        /// last promised ballot within this instance
        promised: Ballot,

        /// thus we need to wait for the final ballot's quorum to proceed
        /// to the final state
        proposed: (Ballot, Bytes),

        /// Set of acceptors that have sent ACCEPTED responses for this instance
        /// (only used when current node is the proposer)
        quorum: QuorumSet,
    },
    /// A final value has been chosen by a quorum of acceptors
    Resolved {
        /// Final ballot that was committed
        accepted: Ballot,
        /// Accepted value
        value: Bytes,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_prepare() {
        let mut acceptor = Acceptor::new(None, 2);

        // acceptor promises the ballot when nothing promised
        let res = acceptor.receive_prepare(Ballot(100, 1));
        assert_eq!(
            res,
            PrepareResponse::Promise {
                proposed: Ballot(100, 1),
                value: None
            }
        );
        assert_eq!(acceptor.promised(), Some(Ballot(100, 1)));

        // acceptor promises higher ballots
        let res = acceptor.receive_prepare(Ballot(102, 2));
        assert_eq!(
            res,
            PrepareResponse::Promise {
                proposed: Ballot(102, 2),
                value: None
            }
        );
        assert_eq!(acceptor.promised(), Some(Ballot(102, 2)));

        // acceptor will reject ballots < promised
        let res = acceptor.receive_prepare(Ballot(101, 1));
        assert_eq!(
            res,
            PrepareResponse::Reject {
                proposed: Ballot(101, 1),
                preempted: Ballot(102, 2)
            }
        );
        assert_eq!(acceptor.promised(), Some(Ballot(102, 2)));

        // prepare contains last accepted values
        acceptor.state = AcceptorState::AwaitQuorum {
            promised: Ballot(102, 2),
            proposed: (Ballot(102, 2), "123".into()),
            quorum: QuorumSet::with_size(2),
        };

        let res = acceptor.receive_prepare(Ballot(103, 1));
        assert_eq!(
            res,
            PrepareResponse::Promise {
                proposed: Ballot(103, 1),
                value: Some((Ballot(102, 2), "123".into()))
            }
        );
        assert_eq!(acceptor.promised(), Some(Ballot(103, 1)));
    }

    #[test]
    fn receive_accept() {
        let mut acceptor = Acceptor::new(None, 2);

        // acceptor allows ACCEPT without a promise
        let res = acceptor.receive_accept(Ballot(101, 1), "ab".into());
        assert_eq!(
            res,
            AcceptResponse::Accepted {
                proposed: Ballot(101, 1),
                preempted_proposal: None,
            }
        );
        assert_eq!(acceptor.promised(), Some(Ballot(101, 1)));

        // acceptor sends REJECT with ballot less than already accepted
        let res = acceptor.receive_accept(Ballot(100, 3), "cd".into());
        assert_eq!(
            res,
            AcceptResponse::Reject {
                proposed: Ballot(100, 3),
                preempted: Ballot(101, 1),
            }
        );

        // acceptor can send out preempted values
        let res = acceptor.receive_accept(Ballot(103, 4), "bbb".into());
        assert_eq!(
            res,
            AcceptResponse::Accepted {
                proposed: Ballot(103, 4),
                preempted_proposal: Some((Ballot(101, 1), "ab".into())),
            }
        );

        // already resolved during ACCEPT
        acceptor.resolve(Ballot(105, 5), "cde".into());
        let res = acceptor.receive_accept(Ballot(105, 5), "cde".into());
        assert_eq!(res, AcceptResponse::Resolved);

        let mut acceptor = Acceptor::new(None, 2);
        acceptor.receive_prepare(Ballot(100, 4));
        assert_eq!(acceptor.promised(), Some(Ballot(100, 4)));

        // Reject during awaiting value
        let res = acceptor.receive_accept(Ballot(0, 0), "aaa".into());
        assert_eq!(
            res,
            AcceptResponse::Reject {
                proposed: Ballot(0, 0),
                preempted: Ballot(100, 4),
            }
        );
    }

    #[test]
    fn receive_accepted() {
        let mut acceptor = Acceptor::new(None, 3);

        // accepts new ballot
        assert_eq!(
            acceptor.receive_accept(Ballot(90, 0), "abc".into()),
            AcceptResponse::Accepted {
                proposed: Ballot(90, 0),
                preempted_proposal: None,
            }
        );

        // ignores previous ballots from the same acceptor
        acceptor.receive_accepted(1, Ballot(90, 0));
        assert!(!acceptor.resolved());
        acceptor.receive_accepted(1, Ballot(90, 0));
        assert!(!acceptor.resolved());

        // ignores ballots not being sent for accepted
        acceptor.receive_accepted(2, Ballot(80, 0));
        assert!(!acceptor.resolved());

        // allows quorum to be reached
        acceptor.receive_accepted(2, Ballot(90, 0));
        assert!(acceptor.resolved());
        assert_eq!(acceptor.resolution(), Some((Ballot(90, 0), "abc".into())));

        // ignores subsequent requests after quorum
        acceptor.receive_accepted(3, Ballot(90, 0));
        assert!(acceptor.resolved());
    }
}
