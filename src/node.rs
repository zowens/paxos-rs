use crate::{
    acceptor::{AcceptResponse, PrepareResponse},
    commands::*,
    proposer::{Proposer, ProposerState},
    window::{DecisionSet, SlotMutRef, SlotWindow},
    Ballot, Configuration, NodeId, Replica, Slot,
};
use bytes::Bytes;
use std::mem;

/// State manager for multi-paxos group
pub struct Node<T> {
    transport: T,
    config: Configuration,
    proposer: Proposer,
    window: SlotWindow,
}

impl<T: Transport> Node<T> {
    /// Node creation from a sender and starting configuration
    pub fn new(transport: T, config: Configuration) -> Node<T> {
        let (p1_quorum, p2_quorum) = config.quorum_size();
        let node = config.current();
        Node {
            transport,
            config,
            proposer: Proposer::new(node, p1_quorum),
            window: SlotWindow::new(p2_quorum),
        }
    }

    /// Broadcast ACCEPT messages once the proposer has phase 1 quorum
    fn drive_accept(&mut self) {
        if !self.proposer.state().is_leader() {
            return;
        }

        let bal = self.proposer.highest_observed_ballot().unwrap();
        assert!(bal.1 == self.config.current());

        // add queued proposals to new slots
        for value in self.proposer.take_proposals() {
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
            self.broadcast(Command::Accept(bal, accepts));
        }
    }

    /// Forwards pending proposals to the new leader
    fn forward(&mut self) {
        if !self.proposer.state().is_follower() || self.proposer.is_proposal_queue_empty() {
            return;
        }

        let proposals = self.proposer.take_proposals();
        if let Some(Ballot(_, node)) = self.proposer.highest_observed_ballot() {
            for proposal in proposals.into_iter() {
                self.send(node, Command::Proposal(proposal));
            }
        }
    }

    #[inline(always)]
    fn send(&mut self, node: NodeId, cmd: Command) {
        self.transport.send(node, &self.config[node], cmd)
    }

    #[inline(always)]
    fn broadcast(&mut self, cmd: Command) {
        for node in self.config.peer_node_ids().into_iter() {
            self.transport.send(node, &self.config[node], cmd.clone());
        }
    }
}

impl<T: Transport> Commander for Node<T> {
    fn proposal(&mut self, val: Bytes) {
        // redirect to the distinguished proposer or start PREPARE
        match *self.proposer.state() {
            ProposerState::Follower if self.proposer.highest_observed_ballot().is_none() => {
                // no known proposers, go through prepare cycle
                self.proposer.push_proposal(val);
                self.propose_leadership();
            }
            ProposerState::Follower => {
                let leader_node = self.proposer.highest_observed_ballot().unwrap().1;
                self.send(leader_node, Command::Proposal(val));
            }
            ProposerState::Candidate { .. } => {
                // still waiting for promises, queue up the value
                // TODO: should this re-send some PREPARE messages?
                self.proposer.push_proposal(val);
            }
            ProposerState::Leader { proposal: bal } => {
                // node is the distinguished proposer
                let slot = {
                    let mut slot_ref = self.window.next_slot();
                    slot_ref.acceptor().notice_value(bal, val.clone());
                    slot_ref.slot()
                };
                self.broadcast(Command::Accept(bal, vec![(slot, val.clone())]));
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
                            let node = bal.1;
                            self.transport.send(
                                node,
                                &self.config[node],
                                Command::Reject(node_id, proposed, preempted),
                            );
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
        self.send(bal.1, Command::Promise(node_id, bal, accepted));
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
                    self.send(bal.1, Command::Reject(current_node, proposed, preempted));
                    return;
                }
                _ => {}
            }
        }

        self.send(bal.1, Command::Accepted(current_node, bal, accepted_slots));
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
            self.broadcast(Command::Resolution(bal, resolutions));
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

        // Send catchup for holds in the decision making
        // We can skip catchup if we're caught up and the range only
        // contains one slot
        let range = self.window.open_range();
        if range.end > range.start + 1 {
            let slots = range
                .filter(|slot| {
                    if let SlotMutRef::Resolved(..) = self.window.slot_mut(*slot) {
                        false
                    } else {
                        true
                    }
                })
                .collect::<Vec<Slot>>();
            trace!("Sending catchup for slots {:?}", slots);
            let leader = self.proposer.highest_observed_ballot().unwrap().1;
            let node = self.config.current();
            self.send(leader, Command::Catchup(node, slots));
        }
    }

    fn catchup(&mut self, node: NodeId, mut slots: Vec<Slot>) {
        // TODO: do we want to redirect at this point? Dropping is certainly safer
        if !self.is_leader() {
            return;
        }

        slots.sort();

        let mut buf = Vec::with_capacity(slots.len());
        let mut run_bal: Option<Ballot> = None;

        for slot in slots.into_iter() {
            if let SlotMutRef::Resolved(bal, val) = self.window.slot_mut(slot) {
                // if we hit a run with a different ballot, send the resolutions we have so far
                if let Some(b) = run_bal {
                    if b != bal && !buf.is_empty() {
                        let next_buf_cap = buf.capacity().saturating_sub(buf.len());
                        let send_buf = mem::replace(&mut buf, Vec::with_capacity(next_buf_cap));
                        self.transport.send(
                            node,
                            &self.config[node],
                            Command::Resolution(b, send_buf),
                        );
                    }
                }

                run_bal = Some(bal);
                buf.push((slot, val));
            }
        }

        if !buf.is_empty() && run_bal.is_some() {
            self.send(node, Command::Resolution(run_bal.unwrap(), buf));
        }
    }
}

impl<T: Transport> Replica for Node<T> {
    fn propose_leadership(&mut self) {
        match *self.proposer.state() {
            ProposerState::Candidate { proposal, .. } => self.broadcast(Command::Prepare(proposal)),
            ProposerState::Follower => {
                let bal = self.proposer.prepare();
                self.broadcast(Command::Prepare(bal));
            }
            ProposerState::Leader { proposal } => {
                // TODO: do we want a special sync here? What about periodic bumping ballot?
                self.broadcast(Command::Accept(proposal, vec![]));
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.proposer.state().is_leader()
    }

    fn tick(&mut self) {}

    fn decisions(&self) -> DecisionSet {
        self.window.decisions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeMetadata;
    use lazy_static::lazy_static;
    use std::ops::Index;

    lazy_static! {
        static ref CONFIG: Configuration = Configuration::new(
            4u32,
            vec![
                (0, NodeMetadata::default()),
                (1, NodeMetadata::default()),
                (2, NodeMetadata::default()),
                (3, NodeMetadata::default()),
            ]
            .into_iter(),
        );
    }

    #[test]
    fn node_proposal() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());

        // sent with no existing proposal, kickstarts phase 1
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[0]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[1]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[2]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[3]);
        replica.transport.clear();

        replica.proposal("456".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        assert!(replica.transport[0].is_empty());
        assert!(replica.transport[1].is_empty());
        assert!(replica.transport[2].is_empty());
        assert!(replica.transport[3].is_empty());

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_proposal_redirection() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.prepare(Ballot(0, 3));
        assert_eq!(Some(Ballot(0, 3)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        replica.proposal("123".into());
        assert!(replica.transport[0].is_empty());
        assert!(replica.transport[1].is_empty());
        assert!(replica.transport[2].is_empty());
        assert_eq!(&[Command::Proposal("123".into())], &replica.transport[3]);

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_prepare() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());

        replica.prepare(Ballot(1, 0));
        assert_eq!(Some(Ballot(1, 0)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Promise(4, Ballot(1, 0), Vec::new())], &replica.transport[0]);
        assert!(&replica.transport[1].is_empty());
        assert!(&replica.transport[2].is_empty());
        assert!(&replica.transport[3].is_empty());
        replica.transport.clear();

        replica.prepare(Ballot(0, 2));
        assert_eq!(Some(Ballot(1, 0)), replica.proposer.highest_observed_ballot());
        assert!(&replica.transport[0].is_empty());
        assert!(&replica.transport[1].is_empty());
        assert_eq!(&[Command::Reject(4, Ballot(0, 2), Ballot(1, 0))], &replica.transport[2]);
        assert!(&replica.transport[3].is_empty());

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_promise_without_existing_accepted_value() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(0, Ballot(0, 4), Vec::new());
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));

        replica.promise(2, Ballot(0, 4), Vec::new());

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(Ballot(0, 4), vec![(0, "123".into())])],
                &replica.transport[i]
            )
        });

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_promise_with_existing_accepted_value() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(1, Ballot(0, 4), vec![(0, Ballot(0, 0), "456".into())]);
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));

        replica.promise(2, Ballot(0, 4), vec![]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(Ballot(0, 4), vec![(0, "456".into()), (1, "123".into())])],
                &replica.transport[i]
            )
        });

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_promise_with_slot_holes() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(1, Ballot(0, 4), vec![(2, Ballot(0, 0), "456".into())]);
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));

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
                &replica.transport[i]
            );
        });

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_accept() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.prepare(Ballot(8, 2));
        assert_eq!(Some(Ballot(8, 2)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        // test rejection first for bal < proposer.highest_observed_ballot
        replica.accept(Ballot(1, 1), vec![(0, "123".into())]);
        assert_eq!(&[Command::Reject(4, Ballot(1, 1), Ballot(8, 2))], &replica.transport[1]);
        replica.transport.clear();

        // test replying with accepted message when bal =
        // proposer.highest_observed_ballot
        replica.accept(Ballot(8, 2), vec![(0, "456".into())]);
        assert_eq!(Some(Ballot(8, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(8, 2), vec![0])], &replica.transport[2]);
        replica.transport.clear();

        // test replying with accepted message when bal >
        // proposer.highest_observed_ballot
        replica.accept(Ballot(9, 2), vec![(0, "789".into())]);
        assert_eq!(Some(Ballot(9, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(9, 2), vec![0])], &replica.transport[2]);

        assert!(replica.window.decisions().is_empty());
        replica.transport.clear();

        // try with multiple accepts
        replica.accept(Ballot(10, 2), vec![(1, "foo".into()), (2, "bar".into())]);
        assert_eq!(Some(Ballot(10, 2)), replica.proposer.highest_observed_ballot());
        assert_eq!(&[Command::Accepted(4, Ballot(10, 2), vec![1, 2])], &replica.transport[2]);
    }

    #[test]
    fn node_reject() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.transport.clear();

        replica.reject(2, Ballot(0, 4), Ballot(5, 3));
        assert_eq!(Some(Ballot(5, 3)), replica.proposer.highest_observed_ballot());
        assert!(replica.proposer.state().is_follower());
        assert_eq!(&[Command::Proposal("123".into())], &replica.transport[3]);
        (0..3).for_each(|i| assert!(replica.transport[i].is_empty()));

        assert!(replica.window.decisions().is_empty());
    }

    #[test]
    fn node_accepted() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.proposer.highest_observed_ballot());
        replica.promise(1, Ballot(0, 4), vec![]);
        replica.promise(0, Ballot(0, 4), vec![]);
        replica.promise(2, Ballot(0, 4), vec![]);
        replica.transport.clear();

        // wait for phase 2 quorum (accepted) before sending resolution
        replica.accepted(0, Ballot(0, 4), vec![0]);
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));

        replica.accepted(2, Ballot(0, 4), vec![0]);
        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(0, "123".into())])],
                &replica.transport[i]
            )
        });

        assert_eq!(vec![(0, "123".into())], replica.window.decisions().iter().collect::<Vec<_>>());

        // allow multiple accepted slots
        replica.proposal("foo".into());
        replica.proposal("bar".into());
        replica.transport.clear();
        replica.accepted(0, Ballot(0, 4), vec![1, 2]);
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));
        replica.accepted(1, Ballot(0, 4), vec![1, 2]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(1, "foo".into()), (2, "bar".into())])],
                &replica.transport[i]
            )
        });

        assert_eq!(
            vec![(0, "123".into()), (1, "foo".into()), (2, "bar".into())],
            replica.window.decisions().iter().collect::<Vec<_>>()
        );

        // allow multiple accepts, but only when the slots receive quorum!
        replica.proposal("foo2".into());
        replica.proposal("bar2".into());
        replica.transport.clear();
        replica.accepted(0, Ballot(0, 4), vec![3, 4]);
        (0..4).for_each(|i| assert!(replica.transport[i].is_empty()));
        replica.accepted(1, Ballot(0, 4), vec![3]);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(Ballot(0, 4), vec![(3, "foo2".into())])],
                &replica.transport[i]
            )
        });

        assert_eq!(
            vec![(0, "123".into()), (1, "foo".into()), (2, "bar".into()), (3, "foo2".into())],
            replica.window.decisions().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn node_resolution() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());

        replica.resolution(Ballot(1, 2), vec![(4, "123".into())]);
        assert_eq!((0..5), replica.window.open_range());
        assert!(match replica.window.slot_mut(4) {
            SlotMutRef::Resolved(Ballot(1, 2), val) if val == "123" => true,
            _ => false,
        });
        assert_eq!(&[Command::Catchup(4, vec![0, 1, 2, 3])], &replica.transport[2]);
        replica.transport.clear();

        replica.resolution(Ballot(1, 2), vec![(1, Bytes::default()), (0, "000".into())]);
        assert_eq!(
            vec![(0, "000".into()), (1, Bytes::default())],
            replica.window.decisions().iter().collect::<Vec<_>>()
        );
        assert_eq!(&[Command::Catchup(4, vec![2, 3])], &replica.transport[2]);
        replica.transport.clear();

        // fill hole 1,2
        replica.resolution(Ballot(1, 2), vec![(2, Bytes::default()), (3, "3".into())]);
        assert!(replica.transport[2].is_empty());

        assert_eq!(
            vec![
                (0, "000".into()),
                (1, Bytes::default()),
                (2, Bytes::default()),
                (3, "3".into()),
                (4, "123".into())
            ],
            replica.window.decisions().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn node_is_leader() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        assert!(!replica.is_leader());

        let bal = replica.proposer.prepare();
        assert!(!replica.is_leader());

        replica.promise(0, bal, vec![]);
        assert!(!replica.is_leader());

        replica.promise(1, bal, vec![]);
        assert!(replica.is_leader());
    }

    #[test]
    fn node_propose_leadership_as_follower() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();

        (0..4).for_each(|i| assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[i]));
    }

    #[test]
    fn node_propose_leadership_as_candidate() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();
        replica.transport.clear();

        replica.propose_leadership();
        (0..4).for_each(|i| assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.transport[i]));
    }

    #[test]
    fn node_propose_leadership_as_leader() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        assert!(!replica.is_leader());
        replica.propose_leadership();
        replica.promise(0, Ballot(0, 4), vec![]);
        replica.promise(1, Ballot(0, 4), vec![]);
        assert!(replica.is_leader());
        replica.transport.clear();

        replica.propose_leadership();
        (0..4).for_each(|i| {
            assert_eq!(&[Command::Accept(Ballot(0, 4), vec![])], &replica.transport[i])
        });
    }

    #[test]
    fn node_catchup() {
        let mut replica = Node::new(VecTransport::default(), CONFIG.clone());
        // put in some slots
        // 0, 1, 2 are resolved
        let resolved_slots = vec![
            (Ballot(0, 1), "123".into()),
            (Ballot(0, 1), "456".into()),
            (Ballot(2, 1), "abc".into()),
        ];
        for (bal, val) in resolved_slots {
            replica.window.next_slot().acceptor().resolve(bal, val);
        }

        // slot 3 is still open
        {
            replica.window.next_slot().acceptor().receive_accept(Ballot(2, 1), "xyz".into());
        }

        // replica that is not the leader cannot respond to catchup
        replica.catchup(2, vec![0, 1, 2]);
        assert!(replica.transport[2].is_empty());

        // make the replica the leader
        assert!(!replica.is_leader());
        replica.propose_leadership();
        (0..=1).for_each(|n| replica.promise(n, Ballot(0, 4), vec![]));
        replica.promise(1, Ballot(0, 4), vec![]);
        assert!(replica.is_leader());
        replica.transport.clear();

        // request catch up for non-closed slots
        replica.catchup(2, vec![3, 4, 5]);
        assert!(replica.transport[2].is_empty());

        // request catchup for open slots
        replica.catchup(2, vec![0, 1, 2, 3]);
        assert_eq!(
            &[
                Command::Resolution(Ballot(0, 1), vec![(0, "123".into()), (1, "456".into())]),
                Command::Resolution(Ballot(2, 1), vec![(2, "abc".into())])
            ],
            &replica.transport[2]
        );

        // resolutions must come in order
        replica.catchup(0, vec![2, 0, 1, 3]);
        assert_eq!(
            &[
                Command::Resolution(Ballot(0, 1), vec![(0, "123".into()), (1, "456".into())]),
                Command::Resolution(Ballot(2, 1), vec![(2, "abc".into())])
            ],
            &replica.transport[0]
        );

        // resolutions can contain holes
        replica.catchup(3, vec![1, 2]);
        assert_eq!(
            &[
                Command::Resolution(Ballot(0, 1), vec![(1, "456".into())]),
                Command::Resolution(Ballot(2, 1), vec![(2, "abc".into())])
            ],
            &replica.transport[3]
        );
    }

    #[derive(Default)]
    struct VecTransport([Vec<Command>; 4]);

    impl VecTransport {
        fn clear(&mut self) {
            for i in 0usize..4 {
                self.0[i].clear();
            }
        }
    }

    impl Index<usize> for VecTransport {
        type Output = [Command];
        fn index(&self, n: usize) -> &[Command] {
            assert!(n < 4);
            &self.0[n]
        }
    }

    impl Transport for VecTransport {
        fn send(&mut self, node: NodeId, _: &NodeMetadata, cmd: Command) {
            assert!(node < 4);
            self.0[node as usize].push(cmd);
        }
    }
}
