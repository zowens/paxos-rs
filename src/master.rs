use super::Instance;
use config::{Configuration, NodeId};
use futures::{Async, Poll, Stream};
use paxos::{Ballot, PaxosInstance};
use std::io;
use std::time::Duration;
use timer::{InstanceResolutionTimer, Scheduler};

// TODO: rename this

/// Actions that `MultiPaxos` must do to utilize the master strategy.
#[derive(Debug)]
pub enum Action {
    /// Send when the current instance needs to issue a PREPARE with
    /// a higher ballot
    Prepare(Instance),
    /// Removes the phase 1 quorum when leadership is lost
    RelasePhaseOneQuorum,
}

/// Action to perform when receiving proposals
pub enum ProposalAction {
    /// Redirect the proposal to another node
    Redirect(NodeId),
    /// Propose within the current node
    CurrentNode,
}

/// Strategy for master status.
pub trait MasterStrategy: Stream<Item = Action, Error = io::Error> {
    /// Forms a new instance of Paxos. Primarily this controls whether or not
    /// the proposer starts out with Phase 1 complete when it has been implicitly
    /// elected to be the distinguished proposer.
    fn next_instance(
        &mut self,
        inst: Instance,
        accepted_bal: Option<Ballot>,
    ) -> PaxosInstance;

    /// Callback when the `inst` instance of Paxos receives an `ACCEPT` message
    fn on_reject(&mut self, inst: Instance);

    /// Callback when the `inst` instance of Paxos receives a `REJECT` message
    fn on_accept(&mut self, inst: Instance);

    /// Action to perform for proposals
    fn proposal_action(&self) -> ProposalAction;
}

/// `MasterStrategy` where no node acts as a master node. All instances must go through
/// Phase 1 of the Paxos algorithm.
pub struct Masterless<S: Scheduler> {
    prepare_timer: InstanceResolutionTimer<S>,
    config: Configuration,
}

impl<S: Scheduler> Masterless<S> {
    /// Creates a new masterless strategy
    pub fn new(config: Configuration, scheduler: S) -> Masterless<S> {
        Masterless {
            prepare_timer: InstanceResolutionTimer::new(scheduler),
            config,
        }
    }

    #[cfg(test)]
    pub(crate) fn prepare_timer(&self) -> &InstanceResolutionTimer<S> {
        &self.prepare_timer
    }
}

impl<S: Scheduler> MasterStrategy for Masterless<S> {
    fn next_instance(
        &mut self,
        _inst: Instance,
        _accepted_bal: Option<Ballot>,
    ) -> PaxosInstance {
        self.prepare_timer.reset();
        PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None)
    }

    #[inline]
    fn on_reject(&mut self, inst: Instance) {
        self.prepare_timer.schedule_retry(inst);
    }

    #[inline]
    fn on_accept(&mut self, inst: Instance) {
        self.prepare_timer.schedule_timeout(inst);
    }

    #[inline]
    fn proposal_action(&self) -> ProposalAction {
        ProposalAction::CurrentNode
    }
}

impl<S: Scheduler> Stream for Masterless<S> {
    type Item = Action;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Action>, io::Error> {
        let timer = try_ready!(self.prepare_timer.poll());
        Ok(Async::Ready(timer.map(Action::Prepare)))
    }
}

// DistinguishedProposer strategy initially starts without a master until an instance is
// accepted with ballot b0.
//
// After instance acceptance, b0.node is given distinguished proposer status for N seconds
// before returning to no master. During this period nodes without DP status forward proposals
// to the distinguished proposer. In addition, b0 is assumed to have promise so as to not
// accept ballots < b0. This aids in rejecting new node proposals.
//
// The distinguished proposer will ignore PREPARE and ACCEPT during this period in order to
// keep leadership role (if a majority of other acceptors accept the competing proposal, the
// DP can either step down or generate a higher ballot to compete). During the distinguished
// proposer status, the DP can skip phase 1 and send ACCEPT messages
// with proposals.
//
// Currently DP status simply expires once the leader timeout has expired. In the future, we can
// gather Phase 1 acceptance for a higher ballot than b0.
//
//                Peer Proposal
//                  ACCEPTED          +------------+
//            +----------------------->            |
//            |                       |  Follower  |
//            |             +---------+            |
//            |             |         +------------+
//            |             |
//            |             |
//            |             | Timeout
//            |             |
//     +------+-------+     |
//     |              |     |
// --->|  Leaderless  <-----+
//     |              |
//     +--^---+-------+
//        |   |                     +----------+
//        |   |                     |          |
//        |   +--------------------->  Leader  |
//        |         Node Proposal   |          |
//        |           ACCEPTED      +----+-----+
//        |                              |
//        |                              |
//        |                              |
//        |                              |
//        |                              |
//        +------------------------------+
//            REJECT, Timeout, or
//            PREPARE with higher ballot
const LEADERSHIP_TIMEOUT: Duration = Duration::from_secs(10);

enum LeadershipState<S: Scheduler> {
    /// No node is currently the master
    Leaderless {
        prepare_timer: InstanceResolutionTimer<S>,
    },
    /// The current node is the leader
    Leader { leadership_timeout: S::Stream },
    /// This node is currently the follower node
    Follower {
        leadership_timeout: S::Stream,
        leader_node: NodeId,
    },
}

/// `MasterStrategy` for keeping a stable leader between instances of Paxos. This strategy
/// has the advantage of utilizing the previous instance's Phase 2 quorum as the implicit
/// Phase 1 quorum for subsequent instances, which reduces the number of messages needed
/// to reach quorum.
pub struct DistinguishedProposer<S: Scheduler> {
    scheduler: S,
    state: LeadershipState<S>,
    config: Configuration,
}

impl<S: Scheduler> DistinguishedProposer<S> {
    pub fn new(config: Configuration, scheduler: S) -> DistinguishedProposer<S> {
        let prepare_timer = InstanceResolutionTimer::new(scheduler.clone());
        DistinguishedProposer {
            scheduler,
            state: LeadershipState::Leaderless { prepare_timer },
            config,
        }
    }
}

impl<S: Scheduler> MasterStrategy for DistinguishedProposer<S> {
    fn next_instance(
        &mut self,
        _inst: Instance,
        accepted_bal: Option<Ballot>,
    ) -> PaxosInstance {
        match accepted_bal {
            // once the node has obtained acceptance, it has leadership of
            // instances >= current_inst.
            Some(ballot) if ballot.1 == self.config.current() => {
                trace!("Current node is Distinguished Proposer");
                let leadership_timeout = self.scheduler.interval(LEADERSHIP_TIMEOUT);
                self.state = LeadershipState::Leader { leadership_timeout };

                PaxosInstance::with_leadership(
                    self.config.current(),
                    self.config.quorum_size(),
                    ballot,
                )
            }
            // followers will use the ballot of this instance as a promise
            // to not accept lower ballots, and recognize the ballot's node
            // value as the distinguished proposer.
            Some(ballot) => {
                let node = ballot.1;
                trace!("Setting Distinguished Proposer to node {:?}", node);
                let leadership_timeout = self.scheduler.interval(LEADERSHIP_TIMEOUT);
                self.state = LeadershipState::Follower {
                    leadership_timeout,
                    leader_node: node,
                };

                // the node inherently "promises" the currently accepted ballot
                // so as to not recognize other nodes that do not have leadership.
                // competing ballots (or, ballots based on the timeout)
                PaxosInstance::new(
                    self.config.current(),
                    self.config.quorum_size(),
                    Some(ballot),
                    None,
                )
            }
            None => {
                self.state = LeadershipState::Leaderless {
                    prepare_timer: InstanceResolutionTimer::new(self.scheduler.clone()),
                };

                PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None)
            }
        }
    }

    fn on_reject(&mut self, inst: Instance) {
        match self.state {
            LeadershipState::Leaderless {
                ref mut prepare_timer,
            } => prepare_timer.schedule_retry(inst),
            LeadershipState::Leader { .. } => {
                debug!("Step down as leader due to REJECT");
                self.state = LeadershipState::Leaderless {
                    prepare_timer: InstanceResolutionTimer::new(self.scheduler.clone()),
                };
            }
            LeadershipState::Follower { .. } => {
                warn!("Received REJECT as follower");
            }
        }
    }

    fn on_accept(&mut self, inst: Instance) {
        if let LeadershipState::Leaderless {
            ref mut prepare_timer,
        } = self.state
        {
            prepare_timer.schedule_timeout(inst);
        }
    }

    #[inline]
    fn proposal_action(&self) -> ProposalAction {
        match self.state {
            LeadershipState::Leaderless { .. } | LeadershipState::Leader { .. } => {
                ProposalAction::CurrentNode
            }
            LeadershipState::Follower { leader_node, .. } => ProposalAction::Redirect(leader_node),
        }
    }
}

impl<S: Scheduler> Stream for DistinguishedProposer<S> {
    type Item = Action;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Action>, io::Error> {
        loop {
            let step_down = match self.state {
                LeadershipState::Leaderless {
                    ref mut prepare_timer,
                } => {
                    let prepare = try_ready!(prepare_timer.poll());
                    return Ok(Async::Ready(prepare.map(Action::Prepare)));
                }
                LeadershipState::Leader {
                    ref mut leadership_timeout,
                } => {
                    try_ready!(leadership_timeout.poll());
                    trace!("Stepping down as leader due to timeout");
                    true
                }
                LeadershipState::Follower {
                    ref mut leadership_timeout,
                    leader_node,
                } => {
                    try_ready!(leadership_timeout.poll());
                    trace!(
                        "Revoking distinguished proposer status from {} due to timeout",
                        leader_node
                    );
                    false
                }
            };

            self.state = LeadershipState::Leaderless {
                prepare_timer: InstanceResolutionTimer::new(self.scheduler.clone()),
            };

            if step_down {
                return Ok(Async::Ready(Some(Action::RelasePhaseOneQuorum)));
            }
        }
    }
}
