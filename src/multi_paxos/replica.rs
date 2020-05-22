use super::commands::*;
use super::Slot;
use crate::config::{Configuration, NodeId};
use crate::paxos::{self, Ballot};
use bytes::Bytes;
use either::Either;

/// State manager for multi-paxos group
pub struct Replica<S> {
    sender: S,
    config: Configuration,
    // TODO: add pipelining
    current: (Slot, paxos::PaxosInstance),
}

impl<S: Sender> Replica<S> {
    // TODO: add overload for start with state snapshot

    /// Replica creation from a sender and starting configuration
    pub fn new(sender: S, config: Configuration) -> Replica<S> {
        let paxos_inst =
            paxos::PaxosInstance::new(config.current(), config.quorum_size(), None, None);
        Replica {
            sender,
            config,
            current: (0, paxos_inst),
        }
    }

    /// Replace the sender with an alertnate implementation
    pub fn sender<A>(self, sender: A) -> Replica<A> {
        Replica {
            sender: sender,
            config: self.config,
            current: self.current,
        }
    }

    pub fn last_promised(&self) -> Option<Ballot> {
        self.current.1.last_promised()
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

    fn next_instance(&mut self, bal: Ballot) {
        let paxos_inst = paxos::PaxosInstance::new(
            self.config.current(),
            self.config.quorum_size(),
            Some(bal),
            None,
        );
        let inst = self.current.0 + 1;
        self.current = (inst, paxos_inst);
    }
}

impl<S: Sender> Commander for Replica<S> {
    fn proposal(&mut self, val: Bytes) {
        let node = self
            .current
            .1
            .last_promised()
            .map(|Ballot(_, node)| node)
            .unwrap_or(self.config.current());

        // redirect to the distinguished proposer
        if node != self.config.current() {
            self.sender.send_to(node, |c| c.proposal(val));
            return;
        }

        match self.current.1.propose_value(val) {
            Some(Either::Left(paxos::Prepare(bal))) => {
                self.broadcast(|c| c.prepare(bal));
            }
            Some(Either::Right(paxos::Accept(bal, val))) => {
                let slot = self.current.0;
                self.broadcast(|c| c.accept(slot, bal, val.clone()));
            }
            None => {
                // TODO: start the next instance
            }
        }
    }

    fn prepare(&mut self, bal: Ballot) {
        let node_id = self.config.current();
        match self.current.1.receive_prepare(bal.1, paxos::Prepare(bal)) {
            Either::Left(paxos::Reply {
                message: paxos::Promise(bal, last_accepted),
                ..
            }) => {
                let last_accepted = last_accepted.map(|paxos::PromiseValue(b, v)| (b, v));
                let slot = self.current.0;
                self.sender
                    .send_to(bal.1, |c| c.promise(node_id, slot, bal, last_accepted));
            }
            Either::Right(paxos::Reply {
                message: paxos::Reject(bal, preempted),
                ..
            }) => {
                self.sender
                    .send_to(bal.1, |c| c.reject(node_id, bal, preempted));
            }
        }
    }

    fn promise(&mut self, node: NodeId, slot: Slot, bal: Ballot, val: Option<(Ballot, Bytes)>) {
        // ignore promises from previous instances
        if slot != self.current.0 {
            return;
        }

        let prom = paxos::Promise(bal, val.map(|(a, b)| paxos::PromiseValue(a, b)));
        if let Some(paxos::Accept(bal, val)) = self.current.1.receive_promise(node, prom) {
            self.broadcast(|c| c.accept(slot, bal, val.clone()));
        }
    }

    fn accept(&mut self, slot: Slot, bal: Ballot, val: Bytes) {
        if slot != self.current.0 {
            return;
        }

        let accept_msg = paxos::Accept(bal, val);
        let current_node = self.config.current();
        match self.current.1.receive_accept(bal.1, accept_msg) {
            Either::Left((paxos::Accepted(bal), _)) => {
                // TODO: is a resolution actually possible here?!
                self.sender
                    .send_to(bal.1, |c| c.accepted(current_node, slot, bal));
            }
            Either::Right(paxos::Reply {
                message: paxos::Reject(proposed, promised),
                ..
            }) => {
                self.sender
                    .send_to(bal.1, |c| c.reject(current_node, proposed, promised));
            }
        }
    }

    fn reject(&mut self, node: NodeId, proposed: Ballot, promised: Ballot) {
        let paxos_reject = paxos::Reject(proposed, promised);
        if let Some(paxos::Prepare(bal)) = self.current.1.receive_reject(node, paxos_reject) {
            self.broadcast(|c| c.prepare(bal));
        }
    }

    fn accepted(&mut self, node: NodeId, slot: Slot, bal: Ballot) {
        if slot != self.current.0 {
            return;
        }

        let paxos_accepted = paxos::Accepted(bal);
        if let Some(paxos::Resolution(bal, val)) =
            self.current.1.receive_accepted(node, paxos_accepted)
        {
            // send out the resolution and advance to the next instance
            self.broadcast(|c| c.resolution(slot, bal, val.clone()));
            self.next_instance(bal);
        }
    }

    fn resolution(&mut self, slot: Slot, bal: Ballot, _val: Bytes) {
        // TODO: need strategy for slot > self current
        if slot != self.current.0 {
            return;
        }
        self.next_instance(bal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;
    use std::ops::Index;

    lazy_static! {
        static ref CONFIG: Configuration = Configuration::new(
            (4u32, "127.0.0.1:4004".parse().unwrap()),
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
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[0]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[1]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[2]);
        assert_eq!(&[Command::Prepare(Ballot(0, 4))], &replica.sender[3]);
        replica.sender.clear();

        // for now, drop proposals while we have an instance in-flight
        // TODO: this should advance to another instance
        replica.proposal("456".into());
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        assert!(replica.sender[0].is_empty());
        assert!(replica.sender[1].is_empty());
        assert!(replica.sender[2].is_empty());
        assert!(replica.sender[3].is_empty());

        // TODO: test proposal after being established as leader
    }

    #[test]
    fn replica_proposal_redirection() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.prepare(Ballot(0, 3));
        assert_eq!(Some(Ballot(0, 3)), replica.last_promised());
        replica.sender.clear();

        replica.proposal("123".into());
        assert!(replica.sender[0].is_empty());
        assert!(replica.sender[1].is_empty());
        assert!(replica.sender[2].is_empty());
        assert_eq!(&[Command::Proposal("123".into())], &replica.sender[3]);
    }

    #[test]
    fn replica_prepare() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());

        replica.prepare(Ballot(1, 0));
        assert_eq!(Some(Ballot(1, 0)), replica.last_promised());
        assert_eq!(
            &[Command::Promise(4, 0, Ballot(1, 0), None)],
            &replica.sender[0]
        );
        assert!(&replica.sender[1].is_empty());
        assert!(&replica.sender[2].is_empty());
        assert!(&replica.sender[3].is_empty());
        replica.sender.clear();

        replica.prepare(Ballot(0, 2));
        assert_eq!(Some(Ballot(1, 0)), replica.last_promised());
        assert!(&replica.sender[0].is_empty());
        assert!(&replica.sender[1].is_empty());
        assert_eq!(
            &[Command::Reject(4, Ballot(0, 2), Ballot(1, 0))],
            &replica.sender[2]
        );
        assert!(&replica.sender[3].is_empty());
    }

    #[test]
    fn replica_promise_without_existing_accepted_value() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        replica.sender.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(0, 0, Ballot(0, 4), None);
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.promise(2, 0, Ballot(0, 4), None);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(0, Ballot(0, 4), "123".into())],
                &replica.sender[i]
            )
        });
    }

    #[test]
    fn replica_promise_with_existing_accepted_value() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        replica.sender.clear();

        // replica needs 2 more promises to achieve Phase 1 Quorum
        replica.promise(1, 0, Ballot(0, 4), Some((Ballot(0, 0), "456".into())));
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.promise(2, 0, Ballot(0, 4), None);

        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Accept(0, Ballot(0, 4), "456".into())],
                &replica.sender[i]
            )
        });
    }

    #[test]
    fn replica_accept() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.prepare(Ballot(8, 2));
        assert_eq!(Some(Ballot(8, 2)), replica.last_promised());
        replica.sender.clear();

        // test rejection first for bal < last_promised
        replica.accept(0, Ballot(1, 1), "123".into());
        assert_eq!(
            &[Command::Reject(4, Ballot(1, 1), Ballot(8, 2))],
            &replica.sender[1]
        );
        replica.sender.clear();

        // test replying with accepted message when bal = last_promised
        replica.accept(0, Ballot(8, 2), "456".into());
        assert_eq!(Some(Ballot(8, 2)), replica.last_promised());
        assert_eq!(&[Command::Accepted(4, 0, Ballot(8, 2))], &replica.sender[2]);
        replica.sender.clear();

        // test replying with accepted message when bal > last_promised
        replica.accept(0, Ballot(9, 2), "789".into());
        assert_eq!(Some(Ballot(9, 2)), replica.last_promised());
        assert_eq!(&[Command::Accepted(4, 0, Ballot(9, 2))], &replica.sender[2]);
    }

    #[test]
    fn replica_reject() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        replica.sender.clear();

        // 1 replica does not meet rejection quorum
        replica.reject(0, Ballot(0, 4), Ballot(5, 2));
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        // 2 replicas not meet rejection quorum
        replica.reject(1, Ballot(0, 4), Ballot(5, 2));
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        // 3 replicas makes rejection quorum
        replica.reject(2, Ballot(0, 4), Ballot(5, 2));
        (0..4).for_each(|i| assert_eq!(&[Command::Prepare(Ballot(5, 4))], &replica.sender[i]));
    }

    #[test]
    fn replica_accepted() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());
        replica.proposal("123".into());
        assert_eq!(Some(Ballot(0, 4)), replica.last_promised());
        replica.promise(1, 0, Ballot(0, 4), None);
        replica.promise(0, 0, Ballot(0, 4), None);
        replica.promise(2, 0, Ballot(0, 4), None);
        replica.sender.clear();

        // wait for phase 2 quorum (accepted) before sending resolution
        replica.accepted(0, 0, Ballot(0, 4));
        (0..4).for_each(|i| assert!(replica.sender[i].is_empty()));

        replica.accepted(2, 0, Ballot(0, 4));
        (0..4).for_each(|i| {
            assert_eq!(
                &[Command::Resolution(0, Ballot(0, 4), "123".into())],
                &replica.sender[i]
            )
        });
    }

    #[test]
    fn replica_resolution() {
        let mut replica = Replica::new(VecSender::default(), CONFIG.clone());

        // ignore slots != current for now
        replica.resolution(4, Ballot(1, 2), "123".into());
        assert_eq!(0, replica.current.0);
        assert_eq!(None, replica.last_promised());
    }

    #[derive(Default)]
    struct VecSender([Vec<Command>; 4]);

    impl VecSender {
        fn clear(&mut self) {
            for i in 0usize..4 {
                self.0[i].clear();
            }
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

        fn send_to<F>(&mut self, node: NodeId, f: F)
        where
            F: FnOnce(&mut Self::Commander) -> (),
        {
            assert!(node < 4);
            f(&mut self.0[node as usize]);
        }
    }
}
