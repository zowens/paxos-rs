use crate::{Ballot, Commander, DecisionSet, NodeId, Replica, Slot};
use bytes::Bytes;

/// A state machine that executes sequentially applied commands.
pub trait ReplicatedState {
    /// Apply a value to the state machine.
    ///
    /// Values are applied in increasing _slot_ order. There may be holes
    /// such that there is no guarantee that _slot-1_ has been
    /// applied before _slot_.
    fn execute(&mut self, slot: Slot, command: Bytes);
}

/// Replica that executes commands within a state machine
pub struct StateMachineReplica<R: Replica, S: ReplicatedState> {
    inner: R,
    state_machine: S,
    next_execution_slot: Slot,
}

impl<R: Replica, S: ReplicatedState> StateMachineReplica<R, S> {
    pub(crate) fn new(replica: R, state_machine: S) -> StateMachineReplica<R, S> {
        StateMachineReplica { inner: replica, state_machine, next_execution_slot: 0 }
    }

    fn try_execute_slots(&mut self) {
        let mut next_slot = self.next_execution_slot;
        let decided = self.decisions().range(self.next_execution_slot..).collect::<Vec<_>>();
        for (slot, decision) in decided {
            if !decision.is_empty() {
                self.state_machine.execute(slot, decision)
            }
            next_slot = slot + 1;
        }
        self.next_execution_slot = next_slot;
    }
}

impl<R: Replica, S: ReplicatedState> Commander for StateMachineReplica<R, S> {
    fn proposal(&mut self, val: Bytes) {
        self.inner.proposal(val);
    }

    fn prepare(&mut self, bal: Ballot) {
        self.inner.prepare(bal);
    }

    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>) {
        self.inner.promise(node, bal, accepted);
    }

    fn accept(&mut self, bal: Ballot, slot_values: Vec<(Slot, Bytes)>) {
        self.inner.accept(bal, slot_values);
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot) {
        self.inner.reject(node, proposed, preempted);
    }

    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>) {
        self.inner.accepted(node, bal, slots);
        self.try_execute_slots();
    }

    fn resolution(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>) {
        self.inner.resolution(bal, values);
        self.try_execute_slots();
    }

    fn catchup(&mut self, node: NodeId, slots: Vec<Slot>) {
        self.inner.catchup(node, slots);
    }
}

impl<R: Replica, S: ReplicatedState> Replica for StateMachineReplica<R, S> {
    fn propose_leadership(&mut self) {
        self.inner.propose_leadership();
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn decisions(&self) -> DecisionSet {
        self.inner.decisions()
    }

    fn tick(&mut self) {
        self.inner.tick();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        commands::{Command, Commander},
        window::{DecisionSet, SlotWindow},
    };

    #[test]
    fn resolve_executes_decisions() {
        let mut inner_replica = FakeReplica(SlotWindow::new(2));
        {
            inner_replica.0.next_slot().acceptor().resolve(Ballot(1, 1), "0".into());
        }
        {
            inner_replica.0.next_slot().acceptor().resolve(Ballot(1, 1), "1".into());
        }
        {
            inner_replica
                .0
                .slot_mut(3)
                .unwrap_empty()
                .fill()
                .acceptor()
                .resolve(Ballot(2, 2), "2".into());
        }

        let mut replica = StateMachineReplica::new(inner_replica, VecStateMachine::default());
        replica.resolution(Ballot(2, 2), vec![]);
        assert_eq!(vec![(0u64, Bytes::from("0")), (1, Bytes::from("1"))], replica.state_machine.0);
        replica.state_machine.0.clear();

        // does not happen again
        replica.resolution(Ballot(2, 2), vec![]);
        assert!(replica.state_machine.0.is_empty());

        // fill hole in slot 2, freeing 3
        {
            replica
                .inner
                .0
                .slot_mut(2)
                .unwrap_open()
                .acceptor()
                .resolve(Ballot(1, 1), Bytes::default());
        }

        replica.resolution(Ballot(2, 2), vec![]);
        assert_eq!(vec![(3u64, Bytes::from("2"))], replica.state_machine.0);
    }

    #[test]
    fn accepted_executes_decisions() {
        let mut inner_replica = FakeReplica(SlotWindow::new(2));
        {
            inner_replica.0.next_slot().acceptor().resolve(Ballot(1, 1), "0".into());
        }
        {
            inner_replica.0.next_slot().acceptor().resolve(Ballot(1, 1), "1".into());
        }
        {
            inner_replica
                .0
                .slot_mut(3)
                .unwrap_empty()
                .fill()
                .acceptor()
                .resolve(Ballot(2, 2), "2".into());
        }

        let mut replica = StateMachineReplica::new(inner_replica, VecStateMachine::default());
        replica.accepted(0, Ballot(2, 2), vec![]);
        assert_eq!(vec![(0u64, Bytes::from("0")), (1, Bytes::from("1"))], replica.state_machine.0);
        replica.state_machine.0.clear();

        // does not happen again
        replica.accepted(1, Ballot(2, 2), vec![]);
        assert!(replica.state_machine.0.is_empty());

        // fill hole in slot 2, freeing 3
        {
            replica
                .inner
                .0
                .slot_mut(2)
                .unwrap_open()
                .acceptor()
                .resolve(Ballot(1, 1), Bytes::default());
        }

        replica.accepted(2, Ballot(2, 2), vec![]);
        assert_eq!(vec![(3u64, Bytes::from("2"))], replica.state_machine.0);
    }

    #[derive(Default)]
    struct VecStateMachine(Vec<(Slot, Bytes)>);
    impl ReplicatedState for VecStateMachine {
        fn execute(&mut self, slot: Slot, val: Bytes) {
            self.0.push((slot, val))
        }
    }

    struct FakeReplica(SlotWindow);
    impl Extend<Command> for FakeReplica {
        fn extend<T>(&mut self, _iter: T)
        where
            T: IntoIterator<Item = Command>,
        {
        }
    }

    impl Replica for FakeReplica {
        fn propose_leadership(&mut self) {
            unimplemented!();
        }

        fn is_leader(&self) -> bool {
            unimplemented!()
        }

        fn tick(&mut self) {
            unimplemented!()
        }

        fn decisions(&self) -> DecisionSet {
            self.0.decisions()
        }
    }
}
