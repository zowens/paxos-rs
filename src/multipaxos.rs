use std::collections::VecDeque;
use std::io;
use futures::task;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use either::Either;
use messages::*;
use algo::*;
use super::Instance;
use state::*;
use statemachine::*;
use config::*;
use timer::*;
use proposals::*;

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
///
/// # Example
///
/// ```rust
/// # use paxos::{Register, MultiPaxos, Configuration, proposal_channel};
/// let register = Register::default();
/// let (proposal_sink, proposal_stream) = proposal_channel();
/// let config = Configuration::new(
///     (0u32, "127.0.0.1:4000".parse().unwrap()),
///     vec![(1, "127.0.0.1:4001".parse().unwrap()),
///          (2, "127.0.0.1:4002".parse().unwrap())].into_iter());
/// let multipaxos = MultiPaxos::new(proposal_stream, register, config);
/// ```
pub struct MultiPaxos<R: ReplicatedState, S: Scheduler = FuturesScheduler> {
    state_machine: R,
    state_handler: StateHandler,

    instance: Instance,
    paxos: PaxosInstance,
    config: Configuration,

    // downstream is sent out from this node
    downstream: VecDeque<ClusterMessage>,
    downstream_blocked: Option<task::Task>,

    // proposals received async
    proposal_receiver: ProposalReceiver,

    // timers for driving resolution
    retransmit_timer: RetransmitTimer<S>,
    prepare_timer: InstanceResolutionTimer<S>,
    sync_timer: RandomPeerSyncTimer<S>,
}

impl<R: ReplicatedState> MultiPaxos<R, FuturesScheduler> {
    /// Creates a multi-paxos node.
    ///
    /// # Arguments
    /// * `proposal_receiver` - Stream containing proposals for the node
    /// * `state_machine` - The finite state machine used to apply decided commands.
    /// * `config` - The initial membership of the cluster in which the node participats.
    pub fn new(
        proposal_receiver: ProposalReceiver,
        state_machine: R,
        config: Configuration,
    ) -> MultiPaxos<R, FuturesScheduler> {
        MultiPaxos::with_scheduler(FuturesScheduler, proposal_receiver, state_machine, config)
    }
}

impl<R: ReplicatedState, S: Scheduler> MultiPaxos<R, S> {
    /// Creates a multi-paxos node.
    ///
    /// # Arguments
    /// * `scheduler` - Custom scheduler used to schedule delayed tasks.
    /// * `proposal_receiver` - Stream containing proposals for the node
    /// * `state_machine` - The finite state machine used to apply decided commands.
    /// * `config` - The initial membership of the cluster in which the node participats.
    pub fn with_scheduler(
        scheduler: S,
        proposal_receiver: ProposalReceiver,
        mut state_machine: R,
        config: Configuration,
    ) -> MultiPaxos<R, S> {
        let mut state_handler = StateHandler::new();

        let state = state_handler.load().unwrap_or_default();
        let paxos = PaxosInstance::new(
            config.current(),
            config.quorum_size(),
            state.promised,
            state.accepted,
        );

        if let Some(v) = state.current_value.clone() {
            state_machine.apply_value(state.instance, v);
        }

        let retransmit_timer = RetransmitTimer::new(scheduler.clone());
        let sync_timer = RandomPeerSyncTimer::new(scheduler.clone());
        let prepare_timer = InstanceResolutionTimer::new(scheduler);

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
            prepare_timer,
            sync_timer,
        }
    }

    /// Creates stream and sink for network messages.
    pub fn into_networked(self) -> NetworkedMultiPaxos<R, S> {
        NetworkedMultiPaxos { multipaxos: self }
    }

    /// Moves to the next instance with an accepted value
    fn advance_instance(&mut self, instance: Instance, value: Value) {
        self.state_machine.apply_value(instance, value.clone());

        let new_inst = instance + 1;
        self.instance = new_inst;

        info!("Starting instance {}", new_inst);
        self.state_handler.persist(State {
            instance: new_inst,
            current_value: Some(value),
            promised: None,
            accepted: None,
        });
        self.paxos =
            PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None);

        self.retransmit_timer.reset();
        self.prepare_timer.reset();
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
        self.downstream.extend(peers.into_iter().map(move |peer| {
            ClusterMessage {
                peer,
                message: MultiPaxosMessage::Prepare(inst, prepare.clone()),
            }
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
        self.downstream.extend(peers.into_iter().map(move |peer| {
            ClusterMessage {
                peer,
                message: MultiPaxosMessage::Accept(inst, accept.clone()),
            }
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
        self.downstream.extend(peers.into_iter().map(move |peer| {
            ClusterMessage {
                peer,
                message: MultiPaxosMessage::Accepted(inst, accepted.clone()),
            }
        }));

        if let Some(task) = self.downstream_blocked.take() {
            task.notify();
        }
    }

    fn propose_update(&mut self, value: Value) {
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
                warn!("Alrady have a value during proposal phases");
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
            self.prepare_timer.reset();
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
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(promise.message.0),
                    accepted: promise.message.1.clone(),
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
        self.prepare_timer.schedule_retry(inst);
    }

    fn on_accept(&mut self, peer: NodeId, inst: Instance, accept: Accept) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_accept(peer, accept) {
            Either::Left((accepted, None)) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(accepted.0),
                    accepted: Some((accepted.0, accepted.1.clone())),
                });

                self.send_accepted(&accepted);
            }
            Either::Left((accepted, Some(Resolution(_, v)))) => {
                trace!("Quorum after ACCEPT received");

                self.send_accepted(&accepted);
                self.advance_instance(inst, v);
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
        self.prepare_timer.schedule_timeout(inst);
    }

    fn on_accepted(&mut self, peer: NodeId, inst: Instance, accepted: Accepted) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let resol = self.paxos.receive_accepted(peer, accepted);

        // if there is quorum, we can advance to the next instance
        if let Some(Resolution(_, value)) = resol {
            self.advance_instance(inst, value);
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

    fn on_catchup(&mut self, inst: Instance, current: Value) {
        // only accept a catchup value if it is greater than
        // the current instance known to this node
        if inst > self.instance {
            // TODO: this call shouldn't have a random -1 without reason...
            // probably should fix advance_instance logic
            self.advance_instance(inst - 1, current);
        }
    }
}

impl<R: ReplicatedState, S: Scheduler> Sink for MultiPaxos<R, S> {
    type SinkItem = ClusterMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: ClusterMessage) -> StartSend<ClusterMessage, io::Error> {
        let ClusterMessage { peer, message } = msg;
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
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        Ok(Async::Ready(()))
    }
}

#[inline]
fn from_poll<V>(s: Poll<Option<V>, io::Error>) -> io::Result<Option<V>> {
    match s {
        Ok(Async::Ready(Some(v))) => Ok(Some(v)),
        Ok(Async::Ready(None)) | Ok(Async::NotReady) => Ok(None),
        Err(e) => Err(e),
    }
}

impl<R: ReplicatedState, S: Scheduler> Stream for MultiPaxos<R, S> {
    type Item = ClusterMessage;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<ClusterMessage>, io::Error> {
        // poll for retransmission
        while let Some((inst, msg)) = from_poll(self.retransmit_timer.poll())? {
            self.poll_retransmit(inst, msg);
        }

        // poll for retry prepare
        while let Some(inst) = from_poll(self.prepare_timer.poll())? {
            self.poll_restart_prepare(inst);
        }

        // poll for sync
        while from_poll(self.sync_timer.poll())?.is_some() {
            self.poll_syncronization();
        }

        // poll for proposals
        while let Some(value) = from_poll(self.proposal_receiver.poll())? {
            self.propose_update(value);
        }

        // TODO: ignore messages with inst < current
        if let Some(v) = self.downstream.pop_front() {
            Ok(Async::Ready(Some(v)))
        } else {
            self.downstream_blocked = Some(task::current());
            Ok(Async::NotReady)
        }
    }
}

/// Multi-paxos node that receives and sends nodes over a network.
pub struct NetworkedMultiPaxos<R: ReplicatedState, S: Scheduler> {
    multipaxos: MultiPaxos<R, S>,
}

impl<R: ReplicatedState, S: Scheduler> Sink for NetworkedMultiPaxos<R, S> {
    type SinkItem = NetworkMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: NetworkMessage) -> StartSend<NetworkMessage, io::Error> {
        let NetworkMessage { address, message } = msg;
        let peer = match self.multipaxos.config.peer_id(&address) {
            Some(v) => v,
            None => {
                warn!(
                    "Received message from address, but is not in configuration: {}",
                    msg.address
                );
                return Ok(AsyncSink::Ready);
            }
        };

        let send_res = self.multipaxos
            .start_send(ClusterMessage { peer, message })?;
        match send_res {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(ClusterMessage { message, .. }) => {
                Ok(AsyncSink::NotReady(NetworkMessage { address, message }))
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.multipaxos.poll_complete()
    }
}

impl<R: ReplicatedState, S: Scheduler> Stream for NetworkedMultiPaxos<R, S> {
    type Item = NetworkMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<NetworkMessage>, io::Error> {
        loop {
            match try_ready!(self.multipaxos.poll()) {
                Some(ClusterMessage { peer, message }) => {
                    if let Some(address) = self.multipaxos.config.address(peer) {
                        return Ok(Async::Ready(Some(NetworkMessage { address, message })));
                    } else {
                        warn!("Unknown peer {:?}", peer);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }
}
