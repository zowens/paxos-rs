use crate::{config::QuorumSet, Ballot, NodeId};
use std::cmp::max;

/// The proposer is a role within paxos that acts as a coordinator for the instance
/// in that it attempts to elect itself the proposer (leader) for the instance via
/// Phase 1. Once it has received a quorum, it will move to Phase 2 in which is will
/// potentially send an ACCEPT message with a value from the acceptor with the highest
/// accepted value already seen (key to the Paxos algorithm)
pub struct Proposer {
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
    /// Creates new proposer state with the node identifier and the Phase 1 quorum size
    pub fn new(node: NodeId, quorum: usize) -> Proposer {
        Proposer {
            state: ProposerState::Follower,
            highest: None,
            current: node,
            quorum,
        }
    }

    /// Returns the proposer's status as either `Follower`, `Candidate` or `Leader`
    pub fn status(&self) -> ProposerStatus {
        match self.state {
            ProposerState::Candidate { .. } => ProposerStatus::Candidate,
            ProposerState::Follower => ProposerStatus::Follower,
            ProposerState::Leader { .. } => ProposerStatus::Leader,
        }
    }

    /// Overrides the highest seen value, if ballot is the highest seen
    pub fn observe_ballot(&mut self, ballot: Ballot) {
        self.highest = max(Some(ballot), self.highest);

        let ballot_leader = self.highest.unwrap().1 == self.current;

        let lost_leadership = match self.state {
            ProposerState::Candidate { .. } | ProposerState::Leader { .. } if !ballot_leader => {
                true
            }
            _ => false,
        };
        if lost_leadership {
            self.state = ProposerState::Follower;
        }
    }

    /// Highest ballot that the proposer has seen
    pub fn highest_observed_ballot(&self) -> Option<Ballot> {
        self.highest
    }

    /// Prepare sets state to candidate and begins to track promises.
    pub fn prepare(&mut self) -> Ballot {
        let new_ballot = self
            .highest
            .map(|m| m.higher_for(self.current))
            .unwrap_or_else(|| Ballot(0, self.current));

        self.highest = Some(new_ballot);

        // this current node accepts itself as proposer
        let mut promises = QuorumSet::with_size(self.quorum);
        promises.insert(self.current);

        self.state = ProposerState::Candidate {
            proposal: new_ballot,
            promises,
        };

        debug!("Starting prepare with {:?}", new_ballot);

        new_ballot
    }

    /// Handler for REJECT from an acceptor peer. Phase 1 with a higher ballot is returned
    /// if the rejection has quorum.
    pub fn receive_reject(&mut self, peer: NodeId, proposed: Ballot, promised: Ballot) {
        debug!(
            "Received REJECT for {:?} with preempted ballot {:?} from peer {}",
            proposed, promised, peer
        );
        if proposed >= promised {
            warn!(
                "Incorrect order received from peer {}, proposed {:?} >= promised {:?}",
                peer, proposed, promised
            );
            return;
        }

        self.observe_ballot(promised);
    }

    /// Note a promise from a peer. An ACCEPT message is returned if quorum is detected.
    pub fn receive_promise(&mut self, peer: NodeId, proposed: Ballot) {
        debug!("Received PROMISE for {:?} from peer {}", proposed, peer);

        match self.state {
            // if a promise is seen in the candiate state, we check for quorum to enter Phase 2
            ProposerState::Candidate {
                proposal,
                ref mut promises,
                ..
            } if proposal == proposed && !promises.contains(peer) =>
            // only allow matching proposals (we could have restarted Phase 1) and only update when
            // we see a new promise from a new peer
            {
                trace!("New promise from peer received");
                promises.insert(peer);

                if !promises.has_quorum() {
                    return;
                }
            }
            _ => {
                return;
            }
        };

        debug!("Quorum reached for Phase 1 of {:?}", proposed);

        // proposer has quorum from acceptors, upgrade to Leader and start
        // Phase 2 if we already have a value
        self.state = ProposerState::Leader { proposal: proposed };
    }
}

/// Encoding of the Proposer role's state machine
#[derive(Debug)]
enum ProposerState {
    /// Empty state: this proposer is not a candidate (in Phase 1) or
    /// a leader (Phase 2)
    Follower,
    /// Proposer has sent out Phase 1a messages to acceptors to become leader.
    Candidate {
        /// The ballot sent out with the PREPARE message
        proposal: Ballot,
        /// Tracking the PROMISE messages received by acceptors.
        promises: QuorumSet,
    },
    /// Proposer has received quorum of PROMISE messages and has entered Phase 2
    /// and has a master lease.
    Leader {
        /// The ballot to send with ACCEPT messages
        proposal: Ballot,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposerStatus {
    /// Proposer is following another node OR there is no known leader
    Follower,
    /// Proposer is waiting for Phase 1b (PROMISE) messages
    Candidate,
    /// Proposer is assumed to be the leader
    Leader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposer_prepare() {
        let mut proposer = Proposer::new(1, 2);
        assert!(proposer.status() != ProposerStatus::Leader);
        proposer.observe_ballot(Ballot(100, 1));

        assert!(match proposer.prepare() {
            Ballot(101, 1) => true,
            _ => false,
        });

        assert!(match proposer.state {
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ..
            } => true,
            _ => false,
        });

        assert!(proposer.status() != ProposerStatus::Leader);
    }

    #[test]
    fn proposer_receive_promise() {
        let mut proposer = Proposer::new(1, 2);
        proposer.observe_ballot(Ballot(100, 1));

        proposer.prepare();
        assert!(proposer.status() != ProposerStatus::Leader);
        assert_eq!(Some(Ballot(101, 1)), proposer.highest_observed_ballot());

        assert!(match proposer.state {
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ref promises,
                ..
            } if promises.contains(1) => true,
            _ => false,
        });

        proposer.receive_promise(2, Ballot(101, 1));
        assert!(proposer.status() == ProposerStatus::Leader);
        assert_eq!(Some(Ballot(101, 1)), proposer.highest_observed_ballot());
        assert!(match proposer.state {
            ProposerState::Leader {
                proposal: Ballot(101, 1),
            } => true,
            _ => false,
        });
    }

    #[test]
    fn proposer_receive_reject() {
        // start a producer that receives rejections during Phase 1
        let mut proposer = Proposer::new(1, 2);
        // fake observing high ballot
        proposer.observe_ballot(Ballot(100, 1));

        proposer.prepare();
        assert!(match proposer.state {
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ..
            } => true,
            _ => false,
        });

        // receive reject for the wrong ballot
        proposer.receive_reject(3, Ballot(5, 1), Ballot(6, 2));
        assert!(proposer.status() != ProposerStatus::Leader);
        assert_eq!(Some(Ballot(101, 1)), proposer.highest_observed_ballot());
        assert!(match proposer.state {
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ..
            } => true,
            _ => false,
        });

        // receive reject for incorrect ballots
        proposer.receive_reject(3, Ballot(101, 1), Ballot(100, 0));
        assert!(proposer.status() != ProposerStatus::Leader);
        assert_eq!(Some(Ballot(101, 1)), proposer.highest_observed_ballot());
        assert!(match proposer.state {
            ProposerState::Candidate {
                proposal: Ballot(101, 1),
                ..
            } => true,
            _ => false,
        });

        proposer.receive_reject(3, Ballot(101, 1), Ballot(102, 2));
        assert!(proposer.status() != ProposerStatus::Leader);
        assert_eq!(Some(Ballot(102, 2)), proposer.highest_observed_ballot());
        assert!(match proposer.state {
            ProposerState::Follower => true,
            _ => false,
        });
    }
}
