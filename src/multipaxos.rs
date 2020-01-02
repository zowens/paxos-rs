use super::Instance;
use config::*;
use either::Either;
use std::future::Future;
use std::task::{Poll, Context, Waker};
use std::pin::Pin;
use pin_project::pin_project;
use futures::stream::Stream;
use futures::sink::Sink;
use master::*;
use messages::*;
use paxos::*;
use proposals::*;
use state::*;
use statemachine::*;
use std::collections::VecDeque;
use std::io;
use timer::*;
use bytes::Bytes;

/// `MultiPaxos` receives messages and attempts to receive consensus on a replicated
/// value. Multiple instances of the paxos algorithm are chained together.
///
/// `ReplicatedState` is applied at each instance transition.
///
/// `ClientMessage` proposals start the process of replicating the value with
/// consensus from a majority. The `MultiPaxosMessage` values are sent out according
/// to the Paxos protocol to the other peers in the cluster.
///
/// `MultiPaxos` is both a `futures::Stream` and `futures::Sink`. It takes in messages
/// and produces messages for other actors within the system. The algorithm itself
/// is separated from any networking concerns.
#[pin_project]
pub struct MultiPaxos<R: ReplicatedState, M: MasterStrategy> {
    state_machine: R,
    state_handler: StateHandler,

    instance: Instance,
    paxos: PaxosInstance,
    config: Configuration,

    // downstream is sent out from this node
    downstream: VecDeque<ClusterMessage>,
    downstream_blocked: Option<Waker>,

    // proposals received async
    proposal_receiver: ProposalReceiver,

    // timers for driving resolution
    #[pin]
    retransmit_timer: RetransmitTimer<ProposerMsg>,
    #[pin]
    sync_timer: RandomPeerSyncTimer,
    master_strategy: M,
}

impl<R: ReplicatedState, M: MasterStrategy> MultiPaxos<R, M> {
    pub(crate) fn new(
        proposal_receiver: ProposalReceiver,
        mut state_machine: R,
        config: Configuration,
        master_strategy: M,
    ) -> MultiPaxos<R, M> {
        let mut state_handler = StateHandler::new();

        let state = state_handler.load().unwrap_or_default();
        let paxos = PaxosInstance::new(
            config.current(),
            config.quorum_size(),
            state.promised,
            state.accepted.map(|p| PromiseValue(p.0, p.1)),
        );

        if let Some(v) = state.current_value.clone() {
            state_machine.apply_value(state.instance, v);
        }

        let retransmit_timer = RetransmitTimer::default();
        let sync_timer = RandomPeerSyncTimer::default();

        MultiPaxos {
            state_machine,
            state_handler,
            instance: state.instance,
            paxos,
            config,
            downstream: VecDeque::new(),
            downstream_blocked: None,
            proposal_receiver,
            retransmit_timer,
            master_strategy,
            sync_timer,
        }
    }

    /// Moves to the next instance with an accepted value
    fn advance_instance(
        &mut self,
        instance: Instance,
        accepted_bal: Option<Ballot>,
        value: Bytes,
    ) {
        self.state_machine.apply_value(instance, value.clone());

        let new_inst = instance + 1;
        self.instance = new_inst;

        self.paxos = self.master_strategy.next_instance(instance, accepted_bal);

        info!("Starting instance {}", new_inst);
        self.state_handler.persist(State {
            instance: new_inst,
            current_value: Some(value),
            promised: self.paxos.last_promised(),
            accepted: None,
        });

        self.retransmit_timer.reset();

        // unblock blocked task in order to poll timing futures,
        // which may not be triggered already via a message send
        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    #[inline]
    fn send_multipaxos(&mut self, peer: NodeId, message: MultiPaxosMessage) {
        debug!("[SEND] peer={} : {:?}", peer, message);
        self.downstream.push_back(ClusterMessage { peer, message });

        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    /// Broadcasts PREPARE messages to all peers
    fn send_prepare(&mut self, prepare: &Prepare) {
        debug!("[BROADCAST] : {:?}", prepare);

        let peers = self.config.peers();
        let inst = self.instance;
        self.downstream
            .extend(peers.into_iter().map(move |peer| ClusterMessage {
                peer,
                message: MultiPaxosMessage::Prepare(inst, prepare.clone()),
            }));

        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    /// Broadcasts ACCEPT messages to all peers
    fn send_accept(&mut self, accept: &Accept) {
        debug!("[BROADCAST] : {:?}", accept);

        let peers = self.config.peers();
        let inst = self.instance;
        self.downstream
            .extend(peers.into_iter().map(move |peer| ClusterMessage {
                peer,
                message: MultiPaxosMessage::Accept(inst, accept.clone()),
            }));

        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    /// Broadcasts ACCEPTED messages to all peers
    fn send_accepted(&mut self, accepted: &Accepted) {
        debug!("[BROADCAST] : {:?}", accepted);

        let inst = self.instance;
        let peers = self.config.peers();
        self.downstream
            .extend(peers.into_iter().map(move |peer| ClusterMessage {
                peer,
                message: MultiPaxosMessage::Accepted(inst, accepted.clone()),
            }));

        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    fn propose_update(&mut self, value: Bytes) {
        match self.master_strategy.proposal_action() {
            ProposalAction::CurrentNode => {
                let inst = self.instance;
                match self.paxos.propose_value(value) {
                    Some(Either::Left(prepare)) => {
                        info!("Starting Phase 1a with proposed value");
                        self.send_prepare(&prepare);
                        self.retransmit_timer.schedule(inst, Either::Left(prepare));
                    }
                    Some(Either::Right(accept)) => {
                        info!("Starting Phase 2a with proposed value");
                        self.send_accept(&accept);
                        self.retransmit_timer.schedule(inst, Either::Right(accept));
                    }
                    None => {
                        // TODO: pipelining should help with this
                        warn!("Alrady have a value during proposal phases");
                    }
                }
            }
            ProposalAction::Redirect(node) => {
                self.send_multipaxos(node, MultiPaxosMessage::RedirectProposal(value));
            }
        }
    }

    fn poll_retransmit(&mut self, inst: Instance, msg: ProposerMsg) {
        if inst != self.instance {
            // TODO: assert?
            warn!("Retransmit for previous instance dropped");
            self.retransmit_timer.reset();
            return;
        }

        // resend prepare messages to peers
        match msg {
            Either::Left(ref v) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_prepare(v);
            }
            Either::Right(ref v) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_accept(v);
            }
        }
    }

    fn poll_restart_prepare(&mut self, instance: Instance) {
        if instance != self.instance {
            warn!("Restart prepare for previous instance dropped");
            return;
        }

        let prepare = self.paxos.prepare();
        info!("Restarting Phase 1 with {:?}", prepare.0);
        self.send_prepare(&prepare);
        self.retransmit_timer
            .schedule(instance, Either::Left(prepare));
    }

    fn poll_syncronization(&mut self) {
        if let Some(node) = self.config.random_peer() {
            debug!("Sending SYNC request");
            let inst = self.instance;
            self.send_multipaxos(node, MultiPaxosMessage::Sync(inst));
        }
    }

    fn on_prepare(&mut self, peer: NodeId, inst: Instance, prepare: Prepare) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_prepare(peer, prepare) {
            Either::Left(promise) => {
                self.state_handler.persist(State {
                    instance: inst,
                    // TODO: this is definitely wrong
                    current_value: None,
                    promised: Some(promise.message.0),
                    accepted: promise.message.1.clone().map(|pv| (pv.0, pv.1)),
                });

                self.send_multipaxos(
                    promise.reply_to,
                    MultiPaxosMessage::Promise(inst, promise.message),
                );
            }
            Either::Right(reject) => {
                self.send_multipaxos(
                    reject.reply_to,
                    MultiPaxosMessage::Reject(inst, reject.message),
                );
            }
        }
    }

    fn on_promise(&mut self, peer: NodeId, inst: Instance, promise: Promise) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        if let Some(accept) = self.paxos.receive_promise(peer, promise) {
            self.send_accept(&accept);
            self.retransmit_timer.schedule(inst, Either::Right(accept));
        }
    }

    fn on_reject(&mut self, peer: NodeId, inst: Instance, reject: Reject) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        // go back to phase 1 when a quorum of REJECT has been received
        let prepare = self.paxos.receive_reject(peer, reject);
        if let Some(prepare) = prepare {
            self.send_prepare(&prepare);
            self.retransmit_timer.schedule(inst, Either::Left(prepare));
        } else {
            self.retransmit_timer.reset();
        }

        // schedule a retry of PREPARE with a higher ballot
        self.master_strategy.on_reject(inst);
    }

    fn on_accept(&mut self, peer: NodeId, inst: Instance, accept: Accept) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_accept(peer, accept) {
            Either::Left((accepted, None)) => {
                let current_value = self.state_machine.snapshot(inst);
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value,
                    promised: Some(accepted.0),
                    accepted: Some((accepted.0, accepted.1.clone())),
                });

                self.send_accepted(&accepted);
            }
            Either::Left((accepted, Some(Resolution(bal, v)))) => {
                trace!("Quorum after ACCEPT received");

                self.send_accepted(&accepted);
                self.advance_instance(inst, Some(bal), v);

                // prevent setting the prepare timer
                return;
            }
            Either::Right(reject) => {
                self.send_multipaxos(
                    reject.reply_to,
                    MultiPaxosMessage::Reject(inst, reject.message),
                );
            }
        }

        // if the proposer dies before receiving consensus, this node can
        // "pick up the ball" and receive consensus from a quorum of
        // the remaining selectors
        //
        // TODO: should we just set this up during ACCEPTED rather than REJECT
        // and ACCEPTED?
        self.master_strategy.on_accept(inst);
    }

    fn on_accepted(&mut self, peer: NodeId, inst: Instance, accepted: Accepted) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let resol = self.paxos.receive_accepted(peer, accepted);

        // if there is quorum, we can advance to the next instance
        if let Some(Resolution(bal, value)) = resol {
            self.advance_instance(inst, Some(bal), value);
        }
    }

    fn on_sync(&mut self, peer: NodeId, inst: Instance) {
        if self.instance <= inst {
            return;
        }

        // receives SYNC request from a peer to get the present value
        // if the instance known to the peer preceeds the current
        // known instance's value
        //
        // Why is this `self.instance - 1`?
        //
        // The catchup will send the current instance (which may be in-flight)
        // and the value from the last instance.
        if let Some(v) = self.state_machine.snapshot(self.instance - 1) {
            let inst = self.instance;
            self.send_multipaxos(peer, MultiPaxosMessage::Catchup(inst, v));
        }
    }

    fn on_catchup(&mut self, inst: Instance, current: Bytes) {
        // only accept a catchup value if it is greater than
        // the current instance known to this node
        if inst <= self.instance {
            trace!("Cannot catch up on instance <= current instance");
            return;
        }
        // TODO: this call shouldn't have a random -1 without reason...
        // probably should fix advance_instance logic
        self.advance_instance(inst - 1, None, current);
    }
}

impl<R: ReplicatedState, M: MasterStrategy> Sink<ClusterMessage> for MultiPaxos<R, M> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: ClusterMessage) -> Result<(), Self::Error> {
        let ClusterMessage { peer, message } = item;
        debug!("[RECEIVE] peer={}: {:?}", peer, message);
        match message {
            MultiPaxosMessage::Prepare(inst, prepare) => {
                self.on_prepare(peer, inst, prepare);
            }
            MultiPaxosMessage::Promise(inst, promise) => {
                self.on_promise(peer, inst, promise);
            }
            MultiPaxosMessage::Accept(inst, accept) => {
                self.on_accept(peer, inst, accept);
            }
            MultiPaxosMessage::Accepted(inst, accepted) => {
                self.on_accepted(peer, inst, accepted);
            }
            MultiPaxosMessage::Reject(inst, reject) => {
                self.on_reject(peer, inst, reject);
            }
            MultiPaxosMessage::Sync(inst) => {
                self.on_sync(peer, inst);
            }
            MultiPaxosMessage::Catchup(inst, value) => {
                self.on_catchup(inst, value);
            }
            MultiPaxosMessage::RedirectProposal(value) => {
                self.propose_update(value);
            }
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[inline]
fn from_poll<V>(s: Poll<Option<V>>) -> Option<V> {
    match s {
        Poll::Ready(Some(v)) => Ok(Some(v)),
        Poll::Ready(None) | Poll::NotReady => None,
    }
}

impl<R: ReplicatedState, M: MasterStrategy> Stream for MultiPaxos<R, M> {
    type Item = ClusterMessage;

    fn poll_next(self: Pin<&mut Self>, ctx: Context<'_>) -> Poll<Option<ClusterMessage>> {
        // poll for retransmission
        while let Some((inst, msg)) = from_poll(self.retransmit_timer.poll()) {
            self.poll_retransmit(inst, msg);
        }

        // poll for master-related actions
        while let Some(action) = from_poll(self.master_strategy.poll()) {
            match action {
                Action::Prepare(inst) => self.poll_restart_prepare(inst),
                Action::RelasePhaseOneQuorum => self.paxos.revoke_leadership(),
            }
        }

        // poll for sync
        while from_poll(self.sync_timer.poll()).is_some() {
            self.poll_syncronization();
        }

        // poll for proposals
        while let Some(value) = from_poll(self.proposal_receiver.poll())? {
            self.propose_update(value);
        }

        // TODO: ignore messages with inst < current
        if let Some(v) = self.downstream.pop_front() {
            Poll::Ready(Some(v))
        } else {
            self.downstream_blocked = Some(ctx.waker());
            Poll::NotReady
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::{spawn, Notify, NotifyHandle};
    use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;
    use std::time::Duration;

    #[test]
    fn catchup() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // advance when instance > current
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Catchup(2, vec![0x0, 0xe].into()),
            })
            .unwrap();

        // check that the instance was advanced
        assert_eq!(2, multi_paxos.instance);
        assert_eq!(1, multi_paxos.state_machine.0.len());
        assert_eq!(&(1, vec![0x0, 0xe].into()), &multi_paxos.state_machine.0[0]);

        // ignore previous instance catchup values
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Catchup(0, vec![0xff, 0xff, 0xff].into()),
            })
            .unwrap();
        assert_eq!(2, multi_paxos.instance);
        assert_eq!(1, multi_paxos.state_machine.0.len());
        assert_eq!(&(1, vec![0x0, 0xe].into()), &multi_paxos.state_machine.0[0]);

        // no messages result
        assert!(collect_messages(multi_paxos).0.is_empty());
    }

    #[test]
    fn sync() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // force instance to instance=2
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Catchup(2, vec![0x0, 0xe].into()),
            })
            .unwrap();

        // check that the instance was advanced
        assert_eq!(2, multi_paxos.instance);

        // send a sync message
        // (peer 1 at instance=0)
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Sync(0),
            })
            .unwrap();

        // send a sync message that should not get a reply
        // since it is > current instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Sync(5),
            })
            .unwrap();

        let msgs = collect_messages(multi_paxos).0;
        assert_eq!(1, msgs.len());
        assert_eq!(
            ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Catchup(2, vec![0x0, 0xe].into()),
            },
            msgs[0]
        );
    }

    #[test]
    fn propose_value() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );

        let (sink, stream) = proposal_channel();

        let multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        sink.propose(vec![0x0, 0xe].into()).unwrap();

        // get messages that result
        let mut peers = HashSet::new();
        for msg in collect_messages(multi_paxos).0 {
            peers.insert(msg.peer);
            assert_matches!(
                msg.message,
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(0, 0)))
            );
        }

        assert_eq!(2, peers.len());
        assert!(peers.contains(&1));
        assert!(peers.contains(&2));
    }

    #[test]
    fn on_prepare() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // peer 2 sends instance > current
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Prepare(10, Prepare(Ballot(10, 2))),
            })
            .unwrap();

        // peer 1 sends prepare that is promised
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Prepare(0, Prepare(Ballot(5, 1))),
            })
            .unwrap();

        // promise Ballot(5, 1)
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());
        assert_eq!(
            ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Promise(0, Promise(Ballot(5, 1), None)),
            },
            msgs[0]
        );

        // rejects ballot < Ballot(5, 1)
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Prepare(0, Prepare(Ballot(0, 2))),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());
        assert_eq!(
            ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 2), Ballot(5, 1))),
            },
            msgs[0]
        );

        // receive accept then additional promise
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        // ignroe messages from accept (will be tested in another test)
        let (_, mut multi_paxos) = collect_messages(multi_paxos);

        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Prepare(0, Prepare(Ballot(10, 3))),
            })
            .unwrap();

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());

        assert_eq!(
            ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Promise(
                    0,
                    Promise(
                        Ballot(10, 3),
                        Some(PromiseValue(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()))
                    )
                ),
            },
            msgs[0]
        );
    }

    #[test]
    fn on_promise() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );

        let (sink, stream) = proposal_channel();

        let multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        sink.propose(vec![0x0, 0xe, 0xb, 0x11].into()).unwrap();

        // ensure prepare messages sent out
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // ignore promise for wrong instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 5,
                message: MultiPaxosMessage::Promise(10, Promise(Ballot(0, 5), None)),
            })
            .unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(0, msgs.len());

        // get back promises to eventually form quorum
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Promise(0, Promise(Ballot(0, 0), None)),
            })
            .unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(0, msgs.len());

        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Promise(0, Promise(Ballot(0, 0), None)),
            })
            .unwrap();
        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        {
            let mut peers = HashSet::new();
            for m in msgs {
                peers.insert(m.peer);
                assert_eq!(
                    MultiPaxosMessage::Accept(
                        0,
                        Accept(Ballot(0, 0), vec![0x0, 0xe, 0xb, 0x11].into())
                    ),
                    m.message
                );
            }

            assert_eq!(4, peers.len());
            assert!(peers.contains(&1));
            assert!(peers.contains(&2));
            assert!(peers.contains(&3));
            assert!(peers.contains(&4));
        }

        assert_eq!(0, multi_paxos.instance);
    }

    #[test]
    fn on_reject() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );

        let (sink, stream) = proposal_channel();

        let multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        sink.propose(vec![0xab, 0xb, 0x11].into()).unwrap();

        // ensure prepare messages sent out
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // ignore reject for wrong instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 5,
                message: MultiPaxosMessage::Reject(10, Reject(Ballot(0, 0), Ballot(1, 1))),
            })
            .unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(0, msgs.len());

        // get quorum of REJECT for the proposal
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 0), Ballot(1, 1))),
            })
            .unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(0, msgs.len());

        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 0), Ballot(4, 4))),
            })
            .unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(0, msgs.len());

        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 0), Ballot(10, 2))),
            })
            .unwrap();
        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // assert re-prepare
        {
            let mut peers = HashSet::new();
            for m in msgs {
                peers.insert(m.peer);
                assert_eq!(
                    MultiPaxosMessage::Prepare(0, Prepare(Ballot(11, 0))),
                    m.message
                );
            }

            assert_eq!(4, peers.len());
            assert!(peers.contains(&1));
            assert!(peers.contains(&2));
            assert!(peers.contains(&3));
            assert!(peers.contains(&4));
        }

        assert_eq!(0, multi_paxos.instance);
    }

    #[test]
    fn on_accept() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // receive accept for wrong instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accept(
                    5,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        // receive accept from current instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);

        // check that ACCEPTED broadcast out
        {
            let mut peers = HashSet::new();
            for m in msgs {
                peers.insert(m.peer);
                assert_eq!(
                    m.message,
                    MultiPaxosMessage::Accepted(
                        0,
                        Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into())
                    )
                );
            }

            assert_eq!(4, peers.len());
            assert!(peers.contains(&1));
            assert!(peers.contains(&2));
            assert!(peers.contains(&3));
            assert!(peers.contains(&4));
        }

        // reject ACCEPT for ballots < last accept
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(0, 3), vec![0xff, 0xff, 0xff, 0xee].into()),
                ),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());
        assert_eq!(
            ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 3), Ballot(5, 1))),
            },
            msgs[0]
        );

        // no decision
        assert_eq!(0, multi_paxos.instance);
    }

    #[test]
    fn on_accept_quorum2() {
        // special case with quorum of size 2
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(2, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // receive accept for wrong instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accept(
                    5,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        // receive accept from current instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);

        // check that ACCEPTED broadcast out
        {
            let mut peers = HashSet::new();
            for m in msgs {
                peers.insert(m.peer);
                assert_eq!(
                    m.message,
                    MultiPaxosMessage::Accepted(
                        0,
                        Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into())
                    )
                );
            }

            assert_eq!(2, peers.len());
            assert!(peers.contains(&1));
            assert!(peers.contains(&2));
        }

        // decided on value
        assert_eq!(1, multi_paxos.instance);
        assert_eq!(1, multi_paxos.state_machine.0.len());
        assert_eq!(
            (0, vec![0x0, 0x1, 0xff].into()),
            multi_paxos.state_machine.0[0]
        );
    }

    #[test]
    fn on_accepted() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // receive accepted for wrong instance
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accepted(
                    5,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());
        // no decision
        assert_eq!(0, multi_paxos.instance);

        // receive accept from current instance
        //
        // peer 1
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accepted(
                    0,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());
        // no decision
        assert_eq!(0, multi_paxos.instance);

        // peer 2
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Accepted(
                    0,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());
        // no decision
        assert_eq!(0, multi_paxos.instance);

        // peer 3
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accepted(
                    0,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        // decided on the value after quorum
        assert_eq!(1, multi_paxos.instance);
        assert_eq!(1, multi_paxos.state_machine.0.len());
        assert_eq!(
            (0, vec![0x0, 0x1, 0xff].into()),
            multi_paxos.state_machine.0[0]
        );
    }

    #[test]
    fn on_accepted_after_accept() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // receive accept (causes 3 and 0 to be added for tracking of quorum)
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();
        assert_eq!(0, multi_paxos.instance);

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // receive 1 more accepted for quorum
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accepted(
                    0,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        // decided on the value after quorum
        assert_eq!(1, multi_paxos.instance);
        assert_eq!(
            (0, vec![0x0, 0x1, 0xff].into()),
            multi_paxos.state_machine.0[0]
        );
    }

    #[test]
    fn on_accept_after_accepted() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();

        let mut multi_paxos = MultiPaxos::new(
            NoOpScheduler,
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), NoOpScheduler),
        );

        // receive ACCEPTED prior to ACCEPT
        multi_paxos
            .start_send(ClusterMessage {
                peer: 1,
                message: MultiPaxosMessage::Accepted(
                    0,
                    Accepted(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());
        assert_eq!(0, multi_paxos.instance);

        // receive accept (causes 3 and 0 to be added for tracking of quorum)
        // and thus, we now have quorum
        multi_paxos
            .start_send(ClusterMessage {
                peer: 3,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(5, 1), vec![0x0, 0x1, 0xff].into()),
                ),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // decided on the value after quorum
        assert_eq!(1, multi_paxos.instance);
        assert_eq!(
            (0, vec![0x0, 0x1, 0xff].into()),
            multi_paxos.state_machine.0[0]
        );
    }

    #[test]
    fn poll_synchronization() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();
        let scheduler = IndexedScheduler::new();
        let multi_paxos = MultiPaxos::new(
            scheduler.clone(),
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), scheduler.clone()),
        );

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        // trigger sync task
        let stream_index = multi_paxos.sync_timer.stream().index;
        scheduler.trigger(stream_index);

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());

        assert_eq!(MultiPaxosMessage::Sync(0), msgs[0].message);

        let peer = msgs[0].peer;
        assert!(peer >= 1);
        assert!(peer <= 4);
    }

    #[test]
    fn poll_retransmit_prepare() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4004".parse().unwrap()),
                (4, "127.0.0.1:4005".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (proposal, stream) = proposal_channel();
        let scheduler = IndexedScheduler::new();
        let multi_paxos = MultiPaxos::new(
            scheduler.clone(),
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), scheduler.clone()),
        );

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        proposal.propose(vec![0x11, 0xba, 0x44].into()).unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // trigger once...
        let stream_index = multi_paxos.retransmit_timer.stream().unwrap().index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(0, 0))),
                m.message
            );
        }

        // trigger again
        scheduler.trigger(stream_index);

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(0, 0))),
                m.message
            );
        }
    }

    #[test]
    fn poll_retransmit_accept() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(2, config.quorum_size());

        let (proposal, stream) = proposal_channel();
        let scheduler = IndexedScheduler::new();
        let multi_paxos = MultiPaxos::new(
            scheduler.clone(),
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), scheduler.clone()),
        );

        // go to phase 2
        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        proposal.propose(vec![0x11, 0xba, 0x44].into()).unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Promise(0, Promise(Ballot(0, 0), None)),
            })
            .unwrap();

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Accept(0, Accept(Ballot(0, 0), vec![0x11, 0xba, 0x44].into())),
                m.message
            );
        }

        // trigger once...
        let stream_index = multi_paxos.retransmit_timer.stream().unwrap().index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Accept(0, Accept(Ballot(0, 0), vec![0x11, 0xba, 0x44].into())),
                m.message
            );
        }

        // trigger again
        scheduler.trigger(stream_index);

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Accept(0, Accept(Ballot(0, 0), vec![0x11, 0xba, 0x44].into())),
                m.message
            );
        }
    }

    #[test]
    fn poll_reprepare_from_reject() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(2, config.quorum_size());

        let (proposal, stream) = proposal_channel();
        let scheduler = IndexedScheduler::new();
        let multi_paxos = MultiPaxos::new(
            scheduler.clone(),
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), scheduler.clone()),
        );

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert!(msgs.is_empty());

        proposal.propose(vec![0x11, 0xba, 0x44].into()).unwrap();
        let (msgs, mut multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());

        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Reject(0, Reject(Ballot(0, 0), Ballot(2, 2))),
            })
            .unwrap();

        // start with a new ballot
        let stream_index = multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .unwrap()
            .index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(3, 0))),
                m.message
            );
        }

        // check that the retransmit task is the new prepare message
        let stream_index = multi_paxos.retransmit_timer.stream().unwrap().index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(3, 0))),
                m.message
            );
        }

        // check that an addition prepare timer task has a new ballot
        let stream_index = multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .unwrap()
            .index;
        scheduler.trigger(stream_index);

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(2, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(4, 0))),
                m.message
            );
        }
    }

    #[test]
    fn poll_reprepare_from_accept() {
        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4003".parse().unwrap()),
                (4, "127.0.0.1:4004".parse().unwrap()),
            ]
            .into_iter(),
        );
        assert_eq!(3, config.quorum_size());

        let (_, stream) = proposal_channel();
        let scheduler = IndexedScheduler::new();
        let mut multi_paxos = MultiPaxos::new(
            scheduler.clone(),
            stream,
            TestStateMachine::default(),
            config.clone(),
            Masterless::new(config.clone(), scheduler.clone()),
        );

        assert!(multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .is_none());

        multi_paxos
            .start_send(ClusterMessage {
                peer: 2,
                message: MultiPaxosMessage::Accept(
                    0,
                    Accept(Ballot(2, 2), vec![0x01, 0xee].into()),
                ),
            })
            .unwrap();

        // accepted messages
        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());

        // start with a new ballot
        assert!(multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .is_some());
        let stream_index = multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .unwrap()
            .index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(3, 0))),
                m.message
            );
        }

        // check that the retransmit task is the new prepare message
        let stream_index = multi_paxos.retransmit_timer.stream().unwrap().index;
        scheduler.trigger(stream_index);

        let (msgs, multi_paxos) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(3, 0))),
                m.message
            );
        }

        // check that an addition prepare timer task has a new ballot
        let stream_index = multi_paxos
            .master_strategy
            .prepare_timer()
            .stream()
            .unwrap()
            .index;
        scheduler.trigger(stream_index);

        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(4, msgs.len());
        for m in msgs {
            assert_eq!(
                MultiPaxosMessage::Prepare(0, Prepare(Ballot(4, 0))),
                m.message
            );
        }
    }

    #[test]
    fn proposal_redirection() {
        struct RedirectMasterStrategy(Configuration);

        impl MasterStrategy for RedirectMasterStrategy {
            fn next_instance(
                &mut self,
                _inst: Instance,
                _accepted_bal: Option<Ballot>,
            ) -> PaxosInstance {
                PaxosInstance::new(self.0.current(), self.0.quorum_size(), None, None)
            }

            fn on_reject(&mut self, _inst: Instance) {}
            fn on_accept(&mut self, _inst: Instance) {}

            fn proposal_action(&self) -> ProposalAction {
                ProposalAction::Redirect(4)
            }
        }

        impl Stream for RedirectMasterStrategy {
            type Item = Action;
            type Error = io::Error;

            fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
                Ok(Async::NotReady)
            }
        }

        let config = Configuration::new(
            (0u32, "127.0.0.1:4000".parse().unwrap()),
            vec![
                (1, "127.0.0.1:4001".parse().unwrap()),
                (2, "127.0.0.1:4002".parse().unwrap()),
                (3, "127.0.0.1:4003".parse().unwrap()),
                (4, "127.0.0.1:4004".parse().unwrap()),
            ]
            .into_iter(),
        );

        assert_eq!(3, config.quorum_size());

        let (sink, stream) = proposal_channel();
        let multi_paxos = MultiPaxos::new(
            NoOpScheduler {},
            stream,
            TestStateMachine::default(),
            config.clone(),
            RedirectMasterStrategy(config.clone()),
        );

        sink.propose(b"hello".to_vec().into()).unwrap();

        // assert the proposal is redirected
        let (msgs, _) = collect_messages(multi_paxos);
        assert_eq!(1, msgs.len());
        assert_eq!(
            ClusterMessage {
                peer: 4,
                message: MultiPaxosMessage::RedirectProposal(b"hello".to_vec().into()),
            },
            msgs[0]
        );
    }

    fn collect_messages<S: Scheduler, M: MasterStrategy>(
        multi_paxos: MultiPaxos<TestStateMachine, M, S>,
    ) -> (
        Vec<ClusterMessage>,
        MultiPaxos<TestStateMachine, M, S>,
    ) {
        let mut s = spawn(multi_paxos);
        let h = notify_noop();

        let mut msgs = vec![];
        while let Async::Ready(Some(v)) = s.poll_stream_notify(&h, 120).unwrap() {
            msgs.push(v);
        }

        assert!(s.get_ref().downstream_blocked.is_some());

        (msgs, s.into_inner())
    }

    fn notify_noop() -> NotifyHandle {
        struct Noop;

        impl Notify for Noop {
            fn notify(&self, _id: usize) {}
        }

        const NOOP: &'static Noop = &Noop;

        NotifyHandle::from(NOOP)
    }

    #[derive(Default)]
    struct TestStateMachine(Vec<(Instance, Bytes)>);

    impl ReplicatedState for TestStateMachine {
        fn apply_value(&mut self, instance: Instance, value: Bytes) {
            self.0.push((instance, value));
        }

        fn snapshot(&self, _instance: Instance) -> Option<Bytes> {
            self.0.iter().next_back().map(|v| v.1.clone())
        }
    }

    #[derive(Default)]
    struct NoOpStream;
    impl Stream for NoOpStream {
        type Item = ();
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Option<()>, io::Error> {
            Ok(Async::NotReady)
        }
    }

    #[derive(Default, Clone)]
    struct NoOpScheduler;
    impl Scheduler for NoOpScheduler {
        type Stream = NoOpStream;
        fn interval(&mut self, _delay: Duration) -> Self::Stream {
            NoOpStream
        }
    }

    #[derive(Clone)]
    struct IndexedScheduler {
        sends: Rc<RefCell<Vec<UnboundedSender<()>>>>,
    }

    impl IndexedScheduler {
        fn new() -> IndexedScheduler {
            IndexedScheduler {
                sends: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn trigger(&self, index: usize) {
            let sends = self.sends.borrow();
            sends[index].unbounded_send(()).unwrap();
        }
    }

    impl Scheduler for IndexedScheduler {
        type Stream = IndexedSchedulerStream;

        fn interval(&mut self, _delay: Duration) -> Self::Stream {
            let (sink, stream) = unbounded::<()>();
            let mut sends = self.sends.borrow_mut();
            let index = sends.len();
            sends.push(sink);
            IndexedSchedulerStream { index, stream }
        }
    }

    struct IndexedSchedulerStream {
        index: usize,
        stream: UnboundedReceiver<()>,
    }

    impl Stream for IndexedSchedulerStream {
        type Item = ();
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Option<()>, io::Error> {
            self.stream
                .poll()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, ""))
        }
    }
}
