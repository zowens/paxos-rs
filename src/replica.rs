use crate::{
    acceptor::{AcceptResponse, PrepareResponse},
    commands::*,
    proposer::{Proposer, ProposerState},
    window::{SlotMutRef, SlotWindow},
    Ballot, Configuration, LeaderElection, NodeId, ReplicatedState, Slot,
};
use bytes::Bytes;
use std::mem;

/// State manager for multi-paxos group
pub struct Replica<S> {
    sender: S,
    config: Configuration,
    proposer: Proposer,
    window: SlotWindow,

    // TODO: bound the proposal queue
    proposal_queue: Vec<Bytes>,
}

impl<S: Sender> Replica<S> {
    /// Replica creation from a sender and starting configuration
    pub fn new(sender: S, config: Configuration) -> Replica<S> {
        let (p1_quorum, p2_quorum) = config.quorum_size();
        let node = config.current();
        Replica {
            sender,
            config,
            proposer: Proposer::new(node, p1_quorum),
            proposal_queue: Vec::new(),
            window: SlotWindow::new(p2_quorum),
        }
    }

    /// Replace the sender with an alertnate implementation
    pub fn with_sender<A>(self, sender: A) -> Replica<A> {
        Replica {
            sender: sender,
            config: self.config,
            proposer: self.proposer,
            proposal_queue: self.proposal_queue,
            window: self.window,
        }
    }

    /// Mutable reference to the sender
    pub fn sender_mut(&mut self) -> &mut S {
        &mut self.sender
    }

    /// Reference to the sender
    pub fn sender(&self) -> &S {
        &self.sender
    }

    /// Broadcast ACCEPT messages once the proposer has phase 1 quorum
    fn drive_accept(&mut self) {
        if !self.proposer.state().is_leader() {
            return;
        }

        let bal = self.proposer.highest_observed_ballot().unwrap();
        assert!(bal.1 == self.config.current());

        // add queued proposals to new slots
        for value in self.proposal_queue.drain(..) {
            let mut slot = self.window.next_slot();
            slot.acceptor().notice_value(bal, value.clone());
        }

        // queue up all accepts
        let accepts = self
            .window
            .open_range()
            .filter_map(|slot| {
                match self.window.slot_mut(slot) {
                    SlotMutRef::Open(ref mut open_slot) => {
                        if let Some((_, val)) = open_slot.acceptor().highest_value() {
                            // have the acceptor update the highest ballot to this one
                            open_slot.acceptor().notice_value(bal, val.clone());
                            Some((slot, val))
                        } else {
                            open_slot.acceptor().notice_value(bal, Bytes::default());
                            Some((slot, Bytes::default()))
                        }
                    }
                    SlotMutRef::Empty(empty_slot) => {
                        // fill the hole with an empty slot
                        let mut slot = empty_slot.fill();
                        slot.acceptor().notice_value(bal, Bytes::default());
                        Some((slot.slot(), Bytes::default()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();

        // send out the accepts
        if !accepts.is_empty() {
            self.broadcast(|c| c.accept(bal, accepts.clone()));
        }
    }

    /// Forwards pending proposals to the new leader
    fn forward(&mut self) {
        if !self.proposer.state().is_follower() || self.proposal_queue.is_empty() {
            return;
        }

        if let Some(Ballot(_, node)) = self.proposer.highest_observed_ballot() {
            let mut proposals = Vec::new();
            mem::swap(&mut self.proposal_queue, &mut proposals);
            self.sender.send_to(node, move |c| {
                for proposal in proposals.into_iter() {
                    c.proposal(proposal);
                }
            });
        }
    }

    /// Executes commands that have been decided.
    fn execute_decisions(&mut self) {
        for (slot, val) in self.window.drain_decisions() {
            trace!("Resolved slot {}", slot);
            if val.len() > 0 {
                self.sender.state_machine().execute(slot, val);
            }
        }
        trace!("Remaining open slot range: {:?}", self.window.open_range());
    }

    fn broadcast<F>(&mut self, f: F)
    where
        F: Fn(&mut S::Commander) -> (),
    {
        // TODO: thrifty option
        for node in self.config.peers().into_iter() {
            self.sender.send_to(node, &f);
        }
    }
}

impl<S: Sender> Commander for Replica<S> {
    fn proposal(&mut self, val: Bytes) {
        // redirect to the distinguished proposer or start PREPARE
        match *self.proposer.state() {
            ProposerState::Follower if self.proposer.highest_observed_ballot().is_none() => {
                // no known proposers, go through prepare cycle
                self.proposal_queue.push(val);
                self.propose_leadership();
            }
            ProposerState::Follower => {
                self.sender.send_to(self.proposer.highest_observed_ballot().unwrap().1, |c| {
                    c.proposal(val)
                });
            }
            ProposerState::Candidate { .. } => {
                // still waiting for promises, queue up the value
                // TODO: should this re-send some PREPARE messages?
                self.proposal_queue.push(val);
            }
            ProposerState::Leader { proposal: bal } => {
                // node is the distinguished proposer
                let slot = {
                    let mut slot_ref = self.window.next_slot();
                    slot_ref.acceptor().notice_value(bal, val.clone());
                    slot_ref.slot()
                };
                self.broadcast(|c| c.accept(bal, vec![(slot, val.clone())]))
            }
        }
    }

    fn prepare(&mut self, bal: Ballot) {
        self.proposer.observe_ballot(bal);

        let node_id = self.config.current();

        let mut accepted = Vec::new();
        for slot in self.window.open_range() {
            match self.window.slot_mut(slot) {
                SlotMutRef::Open(ref mut open_ref) => {
                    match open_ref.acceptor().receive_prepare(bal) {
                        PrepareResponse::Promise { value: Some((bal, val)), .. } => {
                            accepted.push((slot, bal, val));
                        }
                        PrepareResponse::Reject { proposed, preempted } => {
                            // found a slot that accepted a higher ballot, send the reject
                            self.sender.send_to(bal.1, |c| c.reject(node_id, proposed, preempted));
                            return;
                        }
                        _ => {}
                    }
                }
                SlotMutRef::Resolved(bal, val) => {
                    // TODO: is this the right thing to do here?????
                    accepted.push((slot, bal, val));
                }

                SlotMutRef::Empty(_) => {
                    warn!("Empty slot {} detected in the middle of the open range", slot);
                }
                SlotMutRef::ResolutionTruncated => {
                    unreachable!("Cannot be resolved in the middle of the open range")
                }
            }
        }
        self.sender.send_to(bal.1, move |c| c.promise(node_id, bal, accepted));
    }

    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>) {
        if !self.proposer.state().is_candidate() {
            return;
        }

        self.proposer.receive_promise(node, bal);

        // track highest proposals
        for (slot, bal, val) in accepted.into_iter() {
            match self.window.slot_mut(slot) {
                SlotMutRef::Open(ref mut open_slot) => {
                    open_slot.acceptor().notice_value(bal, val);
                }
                SlotMutRef::Empty(empty_slot) => {
                    empty_slot.fill().acceptor().notice_value(bal, val);
                }
                _ => {}
            }
        }

        // if we have phase 1 quorum, we can send out ACCEPT messages
        self.drive_accept();
    }

    fn accept(&mut self, bal: Ballot, slot_values: Vec<(Slot, Bytes)>) {
        self.proposer.observe_ballot(bal);

        let current_node = self.config.current();
        let mut accepted_slots = Vec::with_capacity(slot_values.len());
        for (slot, val) in slot_values.into_iter() {
            let acceptor_res = match self.window.slot_mut(slot) {
                SlotMutRef::Empty(empty_slot) => {
                    let mut open_slot = empty_slot.fill();
                    open_slot.acceptor().receive_accept(bal, val)
                }
                SlotMutRef::Open(ref mut open_slot) => {
                    open_slot.acceptor().receive_accept(bal, val)
                }
                _ => return,
            };

            match acceptor_res {
                AcceptResponse::Accepted { .. } => {
                    // TODO: what do we do w/ the preempted proposal
                    accepted_slots.push(slot);
                }
                AcceptResponse::Reject { proposed, preempted } => {
                    self.sender.send_to(bal.1, |c| c.reject(current_node, proposed, preempted));
                    return;
                }
                _ => {}
            }
        }

        self.sender.send_to(bal.1, |c| c.accepted(current_node, bal, accepted_slots));
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, promised: Ballot) {
        // reject preempted ballot within the proposer
        self.proposer.receive_reject(node, proposed, promised);
        self.forward();
    }

    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>) {
        self.proposer.observe_ballot(bal);

        // notify each slot of the accepted, collecting resolutions
        let mut resolutions = Vec::with_capacity(slots.len());
        for slot in slots {
            match self.window.slot_mut(slot) {
                SlotMutRef::Open(ref mut open_ref) => {
                    open_ref.acceptor().receive_accepted(node, bal);
                    open_ref
                        .acceptor()
                        .resolution()
                        .into_iter()
                        .for_each(|res| resolutions.push((slot, res.1)));
                }
                SlotMutRef::Empty(_) => {
                    warn!("Received accepted() for slot {} which is unknown", slot);
                }
                _ => return,
            }
        }

        if !resolutions.is_empty() {
            resolutions.shrink_to_fit();
            self.broadcast(|c| c.resolution(bal, resolutions.clone()));
            self.execute_decisions();
        }
    }

    fn resolution(&mut self, bal: Ballot, slot_vals: Vec<(Slot, Bytes)>) {
        self.proposer.observe_ballot(bal);

        for (slot, val) in slot_vals.into_iter() {
            match self.window.slot_mut(slot) {
                SlotMutRef::Empty(empty_slot) => empty_slot.fill().acceptor().resolve(bal, val),
                SlotMutRef::Open(ref mut open) => open.acceptor().resolve(bal, val),
                _ => {}
            }
        }

        // execute resolved decisions
        self.execute_decisions();
    }
}

impl<S: Sender> LeaderElection for Replica<S> {
    fn propose_leadership(&mut self) {
        match *self.proposer.state() {
            ProposerState::Candidate { proposal, .. } => self.broadcast(|c| c.prepare(proposal)),
            ProposerState::Follower => {
                let bal = self.proposer.prepare();
                self.broadcast(|c| c.prepare(bal));
            }
            ProposerState::Leader { proposal } => {
                // TODO: do we want a special sync here?
                self.broadcast(|c| c.accept(proposal, vec![]));
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.proposer.state().is_leader()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReplicatedState;
    use lazy_static::lazy_static;
    use std::ops::Index;

    lazy_static! {
        static ref CONFIG: Configuration = Configuration::new(
            4u32,
            vec![
                (0, "127.0.0.1:4000".parse().unwrap()),
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4003".parse().unwrap()),
            ]
            .into_iter(),
        );
    }

    #[test]
    fn replica_proposal() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());

        // sent with no existing proposal, kickstarts phase 1
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[0]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[1]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[2]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[3]);
        replica.sender.clear();

        replica.proposal("456".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        assert!(replica.sender[0].is_empty());
        assert!(replica.sender[1].is_empty());
        assert!(replica.sender[2].is_empty());
        assert!(replica.sender[3].is_empty());

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_proposal_redirection() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.prepare(Ballot(0, 3));
        assert_eq!(Some(Ballot(0, 3)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        replica.proposal("123".into());
        assert!(replica.sender[0].is_empty());
        assert!(replica.sender[1].is_empty());
        assert!(replica.sender[2].is_empty());
        assert_eq!(&[Command::Proposal("123".into())], &replica.sender[3]);

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_prepare() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());

        replica.prepare(Ballot(1, 0));
        assert_eq!(Some(Ballot(1, 0)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Promise(4, Ballot(1, 0), Vec::new())], &replica.sender[0]);
        assert!(&replica.sender[1].is_empty());
        assert!(&replica.sender[2].is_empty());
        assert!(&replica.sender[3].is_empty());
        replica.sender.clear();

        replica.prepare(Ballot(0, 2));
        assert_eq!(Some(Ballot(1, 0)), replica.proposer.highest_observed_ballot());
        assert!(&replica.sender[0].is_empty());
        assert!(&replica.sender[1].is_empty());
        assert_eq!(&[Command::Reject(4, Ballot(0, 2), Ballot(1, 0))], &replica.sender[2]);
        assert!(&replica.sender[3].is_empty());

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_promise_without_existing_accepted_value() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(0, Ballot(0, 4), Vec::new());
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.promise(2, Ballot(0, 4), Vec::new());

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(Ballot(0, 4), vec![(0, "123".into())])],
                &replica.sender[i]
            )
        });

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_promise_with_existing_accepted_value() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(1, Ballot(0, 4), vec![(0, Ballot(0, 0), "456".into())]);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.promise(2, Ballot(0, 4), vec![]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(Ballot(0, 4), vec![(0, "456".into()), (1, "123".into())])],
                &replica.sender[i]
            )
        });

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_promise_with_slot_holes() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(1, Ballot(0, 4), vec![(2, Ballot(0, 0), "456".into())]);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.promise(2, Ballot(0, 4), vec![]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(
                    Ballot(0, 4),
                    vec![
                        (0, Bytes::default()),
                        (1, Bytes::default()),
                        (2, "456".into()),
                        (3, "123".into())
                    ]
                )],
                &replica.sender[i]
            );
        });

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_accept() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.prepare(Ballot(8, 2));
        assert_eq!(Some(Ballot(8, 2)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        // test rejection first for bal < proposer.highest_observed_ballot
        replica.accept(Ballot(1, 1), vec![(0, "123".into())]);
        assert_eq!(&[Command::Reject(4, Ballot(1, 1), Ballot(8, 2))], &replica.sender[1]);
        replica.sender.clear();

        // test replying with accepted message when bal =
        // proposer.highest_observed_ballot
        replica.accept(Ballot(8, 2), vec![(0, "456".into())]);
        assert_eq!(Some(Ballot(8, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(8, 2), vec![0])], &replica.sender[2]);
        replica.sender.clear();

        // test replying with accepted message when bal >
        // proposer.highest_observed_ballot
        replica.accept(Ballot(9, 2), vec![(0, "789".into())]);
        assert_eq!(Some(Ballot(9, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(9, 2), vec![0])], &replica.sender[2]);

        assert!(replica.sender.resolutions().is_empty());
        replica.sender.clear();

        // try with multiple accepts
        replica.accept(Ballot(10, 2), vec![(1, "foo".into()), (2, "bar".into())]);
        assert_eq!(Some(Ballot(10, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(10, 2), vec![1, 2])], &replica.sender[2]);
    }

    #[test]
    fn replica_reject() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.sender.clear();

        replica.reject(2, Ballot(0, 4), Ballot(5, 3));
        assert_eq!(Some(Ballot(5, 3)), replica.proposer.highest_observed_ballot());
        assert!(replica.proposer.state().is_follower());
        assert_eq!(&[Command::Proposal("123".into())], &replica.sender[3]);
        (0..3).for_each(|i| assert!(replica.sender[i].is_empty()));

        assert!(replica.sender.resolutions().is_empty());
    }

    #[test]
    fn replica_accepted() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.promise(1, Ballot(0, 4), vec![]);
        replica.promise(0, Ballot(0, 4), vec![]);
        replica.promise(2, Ballot(0, 4), vec![]);
        replica.sender.clear();

        // wait for phase 2 quorum (accepted) before sending resolution
        replica.accepted(0, Ballot(0, 4), vec![0]);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.accepted(2, Ballot(0, 4), vec![0]);
        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(0, "123".into())])],
                &replica.sender[i]
            )
        });

        assert_eq!(&[(0, "123".into())], replica.sender.resolutions());

        // allow multiple accepted slots
        replica.proposal("foo".into());
        replica.proposal("bar".into());
        replica.sender.clear();
        replica.accepted(0, Ballot(0, 4), vec![1, 2]);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));
        replica.accepted(1, Ballot(0, 4), vec![1, 2]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(1, "foo".into()), (2, "bar".into())])],
                &replica.sender[i]
            )
        });

        assert_eq!(
            &[(0, "123".into()), (1, "foo".into()), (2, "bar".into())],
            replica.sender.resolutions()
        );

        // allow multiple accepts, but only when the slots receive quorum!
        replica.proposal("foo2".into());
        replica.proposal("bar2".into());
        replica.sender.clear();
        replica.accepted(0, Ballot(0, 4), vec![3, 4]);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));
        replica.accepted(1, Ballot(0, 4), vec![3]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(3, "foo2".into())])],
                &replica.sender[i]
            )
        });

        assert_eq!(
            &[(0, "123".into()), (1, "foo".into()), (2, "bar".into()), (3, "foo2".into())],
            replica.sender.resolutions()
        );
    }

    #[test]
    fn replica_resolution() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());

        replica.resolution(Ballot(1, 2), vec![(4, "123".into())]);
        assert_eq!((0..5), replica.window.open_range());
        assert!(match replica.window.slot_mut(4) {
            SlotMutRef::Resolved(Ballot(1, 2), val) if val == "123" => true,
            _ => false,
        });

        replica.resolution(Ballot(1, 2), vec![(1, Bytes::default()), (0, "000".into())]);
        assert_eq!(&[(0, "000".into())], replica.sender.resolutions());

        // fill hole 1,2
        replica.resolution(Ballot(1, 2), vec![(2, Bytes::default()), (3, "3".into())]);

        assert_eq!(
            &[(0, "000".into()), (3, "3".into()), (4, "123".into())],
            replica.sender.resolutions()
        );
    }

    #[test]
    fn replica_is_leader() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        assert!(!replica.is_leader());

        let bal = replica.proposer.prepare();
        assert!(!replica.is_leader());

        replica.promise(0, bal, vec![]);
        assert!(!replica.is_leader());

        replica.promise(1, bal, vec![]);
        assert!(replica.is_leader());
    }

    #[test]
    fn replica_propose_leadership_as_follower() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();

        (0..4).for_each(|i| assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[i]));
    }

    #[test]
    fn replica_propose_leadership_as_candidate() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();
        replica.sender.clear();

        replica.propose_leadership();
        (0..4).for_each(|i| assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[i]));
    }

    #[test]
    fn replica_propose_leadership_as_leader() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();
        replica.promise(0, Ballot(0, 4), vec![]);
        replica.promise(1, Ballot(0, 4), vec![]);
        assert!(replica.is_leader());
        replica.sender.clear();

        replica.propose_leadership();
        (0..4)
            .for_each(|i| assert_eq!(&[Command::Accept(Ballot(0, 4), vec![])], &replica.sender[i]));
    }

    #[derive(Default)]
    struct VecSender([Vec<Command>; 4], StateMachine);

    impl VecSender {
        fn clear(&mut self) {
            for i in 0usize..4 {
                self.0[i].clear();
            }
        }

        fn resolutions(&self) -> &[(Slot, Bytes)] {
            &(&self.1).0
        }
    }

    impl Index<usize> for VecSender {
        type Output = [Command];
        fn index(&self, n: usize) -> &[Command] {
            assert!(n < 4);
            &self.0[n]
        }
    }

    impl Sender for VecSender {
        type Commander = Vec<Command>;
        type StateMachine = StateMachine;

        fn send_to<F>(&mut self, node: NodeId, f: F)
        where
            F: FnOnce(&mut Self::Commander) -> (),
        {
            assert!(node < 4);
            f(&mut self.0[node as usize]);
        }

        fn state_machine(&mut self) -> &mut Self::StateMachine {
            &mut self.1
        }
    }

    #[derive(Default)]
    struct StateMachine(Vec<(Slot, Bytes)>);

    impl ReplicatedState for StateMachine {
        fn execute(&mut self, slot: Slot, command: Bytes) {
            self.0.push((slot, command));
        }
    }
}
