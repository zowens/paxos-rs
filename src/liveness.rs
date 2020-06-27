use crate::{commands::Commander, Ballot, LeaderElection, NodeId, Slot, Tick};
use bytes::Bytes;
use std::time::{Duration, Instant};

/// Adds liveness to a commander by taking leadership
/// when a timeout occurs
pub struct Liveness<R: Commander + LeaderElection> {
    inner: R,
    leader_election: Timeout,
}

impl<R: Commander + LeaderElection> Liveness<R> {
    pub fn new(inner: R) -> Liveness<R> {
        Liveness {
            inner,
            // TODO: configurable leadership election timeout
            leader_election: Timeout::new(Duration::from_secs(2)),
        }
    }

    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }
}

impl<R: Commander + LeaderElection> Commander for Liveness<R> {
    fn proposal(&mut self, val: Bytes) {
        self.inner.proposal(val);
    }

    fn prepare(&mut self, bal: Ballot) {
        self.leader_election.bump();
        self.inner.prepare(bal);
    }

    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>) {
        self.leader_election.bump();
        self.inner.promise(node, bal, accepted);
    }

    fn accept(&mut self, bal: Ballot, slot_values: Vec<(Slot, Bytes)>) {
        self.leader_election.bump();
        self.inner.accept(bal, slot_values);
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot) {
        self.leader_election.bump();
        self.inner.reject(node, proposed, preempted);
    }

    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>) {
        self.leader_election.bump();
        self.inner.accepted(node, bal, slots);
    }

    fn resolution(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>) {
        self.leader_election.bump();
        self.inner.resolution(bal, values);
    }
}

impl<R: Commander + LeaderElection> Tick for Liveness<R> {
    fn tick(&mut self) {
        let lapsed = if self.inner.is_leader() {
            self.leader_election.near()
        } else {
            self.leader_election.lapsed()
        };

        if lapsed {
            info!("Leadership timeout lapsed, proposing leadership");
            self.inner.propose_leadership();
            self.leader_election.clear();
        }
    }
}

impl<R: Commander + LeaderElection> LeaderElection for Liveness<R> {
    fn propose_leadership(&mut self) {
        self.inner.propose_leadership();
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }
}

struct Timeout {
    latest_message: Option<Instant>,
    timeout: Duration,
}

impl Timeout {
    fn new(timeout: Duration) -> Timeout {
        Timeout { latest_message: None, timeout }
    }

    fn clear(&mut self) {
        self.latest_message = None;
    }

    fn bump(&mut self) {
        trace!("Leadership timeout bumped");
        self.latest_message = Some(Instant::now());
    }

    fn lapsed(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + self.timeout
        } else {
            false
        }
    }

    fn near(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + (self.timeout / 2)
        } else {
            false
        }
    }

    #[cfg(test)]
    fn fast_forward(&mut self, d: Duration) {
        // "jump" in time for tests by setting the clock back
        self.latest_message = self.latest_message.take().map(|i| i - d - Duration::from_nanos(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;

    #[test]
    fn propose_does_not_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        live.proposal("123".into());

        // does not bump leadership
        assert!(live.leader_election.latest_message.is_none());
        assert_eq!(Command::Proposal("123".into()), live.inner.commands[0]);
    }

    #[test]
    fn commands_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        live.prepare(Ballot(2, 3));
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Prepare(Ballot(2, 3)));

        let mut live = Liveness::new(Inner::default());
        live.promise(0, Ballot(2, 3), vec![]);
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Promise(0, Ballot(2, 3), vec![]));

        let mut live = Liveness::new(Inner::default());
        live.reject(4, Ballot(0, 1), Ballot(4, 5));
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Reject(4, Ballot(0, 1), Ballot(4, 5)));

        let mut live = Liveness::new(Inner::default());
        live.accept(Ballot(4, 5), vec![]);
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Accept(Ballot(4, 5), vec![]));

        let mut live = Liveness::new(Inner::default());
        live.accepted(5, Ballot(1, 2), vec![2, 3, 4]);
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Accepted(5, Ballot(1, 2), vec![2, 3, 4]));

        let mut live = Liveness::new(Inner::default());
        live.resolution(Ballot(1, 2), vec![]);
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Resolution(Ballot(1, 2), vec![]));
    }

    #[test]
    fn tick_leader() {
        let mut live = Liveness::new(Inner::default());
        live.inner.leader = true;
        assert!(live.is_leader());
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.accepted(5, Ballot(1, 2), vec![2, 3, 4]);
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(1));

        live.tick();
        assert!(live.inner.proposed_leadership);
    }

    #[test]
    fn tick_follower() {
        let mut live = Liveness::new(Inner::default());
        live.inner.leader = false;
        assert!(!live.is_leader());
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.resolution(Ballot(0, 1), vec![]);
        live.tick();
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(2));

        live.tick();
        assert!(live.inner.proposed_leadership);
    }

    #[derive(Default)]
    struct Inner {
        commands: Vec<Command>,
        leader: bool,
        proposed_leadership: bool,
    }

    impl Extend<Command> for Inner {
        fn extend<T>(&mut self, iter: T)
        where
            T: IntoIterator<Item = Command>,
        {
            self.commands.extend(iter);
        }
    }

    impl LeaderElection for Inner {
        fn propose_leadership(&mut self) {
            self.proposed_leadership = true;
        }

        fn is_leader(&self) -> bool {
            self.leader
        }
    }
}
