use std::io;
use std::net::SocketAddr;
use std::cmp::min;
use std::time::Duration;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::task;
use messages::*;
use algo::*;
use super::Instance;
use state::*;
use config::*;
use timer::Scheduler;
use rand::{thread_rng, Rng};

/// Starting timeout for restarting Phase 1
const RESOLUTION_STARTING_MS: u64 = 5;
/// Cap on the timeout for restarting Phase 1
const RESOLUTION_MAX_MS: u64 = 2000;
/// Timeout for post-ACCEPT restarting Phase 1
const RESOLUTION_SILENCE_TIMEOUT: u64 = 3000;
/// Periodic synchronization time
const SYNC_TIME_MS: u64 = 5000;

/// `ReplicatedState` is a state machine that applies value synchronously. The
/// value is replicated with `MultiPaxos`.
pub trait ReplicatedState {
    /// Apply a value to the state machine
    fn apply_value(&mut self, instance: Instance, value: Value);

    // TODO: need log semantics
    /// Snapshots the value
    fn snapshot(&self, instance: Instance) -> Option<Value>;
}

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
pub struct MultiPaxos<R: ReplicatedState, S: Scheduler> {
    state_machine: R,
    state_handler: StateHandler,

    instance: Instance,
    paxos: PaxosInstance,
    config: Configuration,

    // downstream is sent out from this node
    downstream_sink: UnboundedSender<Message>,
    downstream_stream: UnboundedReceiver<Message>,

    // timers for driving resolution
    retransmit_timer: RetransmitTimer<S>,
    prepare_timer: InstanceResolutionTimer<S>,
    sync_timer: S::Stream,
}

impl<R: ReplicatedState, S: Scheduler> MultiPaxos<R, S> {
    /// Creates a new multi-paxos machine
    pub fn new(mut state_machine: R, mut scheduler: S, config: Configuration) -> MultiPaxos<R, S> {
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

        let (downstream_sink, downstream_stream) = unbounded::<Message>();

        let retransmit_timer = RetransmitTimer::new(scheduler.clone());
        let sync_timer = scheduler.interval(Duration::from_millis(SYNC_TIME_MS));
        let prepare_timer = InstanceResolutionTimer::new(scheduler);

        MultiPaxos {
            state_machine,
            state_handler,
            instance: state.instance,
            paxos,
            config,
            downstream_sink,
            downstream_stream,
            retransmit_timer,
            prepare_timer,
            sync_timer,
        }
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
    fn send_multipaxos(&self, m: MultiPaxosMessage) {
        self.downstream_sink
            .unbounded_send(Message::MultiPaxos(m))
            .unwrap();
    }

    #[inline]
    fn send_client(&self, m: ClientMessage) {
        self.downstream_sink
            .unbounded_send(Message::Client(m))
            .unwrap();
    }

    /// Broadcasts PREPARE messages to all peers
    fn send_prepare(&mut self, prepare: &Prepare) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(MultiPaxosMessage::Prepare(
                self.instance,
                Prepare(peer, prepare.1),
            ));
        }
    }

    /// Broadcasts ACCEPT messages to all peers
    fn send_accept(&mut self, accept: &Accept) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(MultiPaxosMessage::Accept(
                self.instance,
                Accept(peer, accept.1, accept.2.clone()),
            ));
        }
    }

    /// Broadcasts ACCEPTED messages to all peers
    fn send_accepted(&mut self, accepted: &Accepted) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(MultiPaxosMessage::Accepted(
                self.instance,
                Accepted(peer, accepted.1, accepted.2.clone()),
            ));
        }
    }

    fn propose_update(&mut self, value: Value) {
        let inst = self.instance;
        match self.paxos.propose_value(value) {
            Some(ProposerMsg::Prepare(prepare)) => {
                info!("Starting Phase 1a with proposed value");
                self.send_prepare(&prepare);
                self.retransmit_timer
                    .schedule(inst, ProposerMsg::Prepare(prepare));
            }
            Some(ProposerMsg::Accept(accept)) => {
                info!("Starting Phase 2a with proposed value");
                self.send_accept(&accept);
                self.retransmit_timer
                    .schedule(inst, ProposerMsg::Accept(accept));
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
            ProposerMsg::Prepare(v) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_prepare(&v);
            }
            ProposerMsg::Accept(ref v) => {
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
        info!("Restarting Phase 1 with {:?}", prepare.1);
        self.send_prepare(&prepare);
        self.retransmit_timer
            .schedule(instance, ProposerMsg::Prepare(prepare));
    }

    fn poll_syncronization(&mut self) {
        if let Some(node) = self.config.random_peer() {
            debug!("Sending SYNC request");
            self.send_multipaxos(MultiPaxosMessage::Sync(node, self.instance));
        }
    }

    fn on_prepare(&mut self, inst: Instance, prepare: Prepare) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_prepare(prepare) {
            Ok(promise) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(promise.1),
                    accepted: promise.2.clone(),
                });

                self.send_multipaxos(MultiPaxosMessage::Promise(self.instance, promise));
            }
            Err(reject) => {
                self.send_multipaxos(MultiPaxosMessage::Reject(self.instance, reject));
            }
        }
    }

    fn on_promise(&mut self, inst: Instance, promise: Promise) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        if let Some(accept) = self.paxos.receive_promise(promise) {
            self.send_accept(&accept);
            self.retransmit_timer
                .schedule(inst, ProposerMsg::Accept(accept));
        }
    }

    fn on_reject(&mut self, inst: Instance, reject: Reject) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        // go back to phase 1 when a quorum of REJECT has been received
        let prepare = self.paxos.receive_reject(reject);
        if let Some(prepare) = prepare {
            self.send_prepare(&prepare);
            self.retransmit_timer
                .schedule(inst, ProposerMsg::Prepare(prepare));
        } else {
            self.retransmit_timer.reset();
        }

        // schedule a retry of PREPARE with a higher ballot
        self.prepare_timer.schedule_retry(inst);
    }

    fn on_accept(&mut self, inst: Instance, accept: Accept) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_accept(accept) {
            Ok(accepted @ Accepted(..)) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(accepted.1),
                    accepted: Some((accepted.1, accepted.2.clone())),
                });

                self.send_accepted(&accepted);
            }
            Err(reject) => {
                self.send_multipaxos(MultiPaxosMessage::Reject(self.instance, reject));
            }
        }

        // if the proposer dies before receiving consensus, this node can
        // "pick up the ball" and receive consensus from a quorum of
        // the remaining selectors
        self.prepare_timer.schedule_timeout(inst);
    }

    fn on_accepted(&mut self, inst: Instance, accepted: Accepted) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let resol = self.paxos.receive_accepted(accepted);

        // if there is quorum, we can advance to the next instance
        if let Some(Resolution(_, _, value)) = resol {
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
            self.send_multipaxos(MultiPaxosMessage::Catchup(peer, self.instance, v));
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

    fn on_client_lookup(&mut self, addr: SocketAddr) {
        let inst = self.instance;
        match self.state_machine.snapshot(inst) {
            Some(v) => {
                self.send_client(ClientMessage::CurrentValueResponse(addr, v));
            }
            None => {
                self.send_client(ClientMessage::NoValueResponse(addr));
            }
        }
    }
}

impl<R: ReplicatedState, S: Scheduler> Sink for MultiPaxos<R, S> {
    type SinkItem = Message;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: Message) -> StartSend<Message, io::Error> {
        match msg {
            Message::MultiPaxos(MultiPaxosMessage::Prepare(inst, prepare)) => {
                debug!("Received {:?}", prepare);
                self.on_prepare(inst, prepare);
            }
            Message::MultiPaxos(MultiPaxosMessage::Promise(inst, promise)) => {
                debug!("Received {:?}", promise);
                self.on_promise(inst, promise);
            }
            Message::MultiPaxos(MultiPaxosMessage::Accept(inst, accept)) => {
                debug!("Received {:?}", accept);
                self.on_accept(inst, accept);
            }
            Message::MultiPaxos(MultiPaxosMessage::Accepted(inst, accepted)) => {
                debug!("Received {:?}", accepted);
                self.on_accepted(inst, accepted);
            }
            Message::MultiPaxos(MultiPaxosMessage::Reject(inst, reject)) => {
                debug!("Received {:?}", reject);
                self.on_reject(inst, reject);
            }
            Message::MultiPaxos(MultiPaxosMessage::Sync(peer, inst)) => {
                debug!("Received SYNC from {:?} to instance {:?}", peer, inst);
                self.on_sync(peer, inst);
            }
            Message::MultiPaxos(MultiPaxosMessage::Catchup(peer, inst, value)) => {
                debug!("Received CATCHUP from {:?} to instance {}", peer, inst);
                self.on_catchup(inst, value);
            }
            Message::Client(ClientMessage::ProposeRequest(addr, value)) => {
                debug!("Received PROPOSE request from client {}", addr);
                self.propose_update(value);
            }
            Message::Client(ClientMessage::LookupValueRequest(addr)) => {
                debug!("Received GET request from client {}", addr);
                self.on_client_lookup(addr);
            }
            Message::Client(req) => {
                warn!("Received unknown client request {:?}", req);
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
    type Item = Message;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<Message>, io::Error> {
        // poll for retransmission
        if let Some((inst, msg)) = from_poll(self.retransmit_timer.poll())? {
            self.poll_retransmit(inst, msg);
        }

        // poll for retry prepare
        if let Some(inst) = from_poll(self.prepare_timer.poll())? {
            self.poll_restart_prepare(inst);
        }

        // poll for sync
        if from_poll(self.sync_timer.poll())?.is_some() {
            self.poll_syncronization();
        }

        self.downstream_stream
            .poll()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "unexpected timer error"))
    }
}

/// Timer that will resend a message to the downstream peers in order
/// to drive consensus.
struct RetransmitTimer<S: Scheduler> {
    scheduler: S,
    msg: Option<(Instance, ProposerMsg, S::Stream)>,
    parked_receive: Option<task::Task>,
}

impl<S> RetransmitTimer<S>
where
    S: Scheduler,
{
    pub fn new(scheduler: S) -> RetransmitTimer<S> {
        RetransmitTimer {
            scheduler,
            msg: None,
            parked_receive: None,
        }
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.msg = None;
    }

    /// Schedules a message for resend
    pub fn schedule(&mut self, inst: Instance, msg: ProposerMsg) {
        self.msg = Some((
            inst,
            msg,
            self.scheduler.interval(Duration::from_millis(1000)),
        ));

        // notify any parked receive
        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }
}

impl<S: Scheduler> Stream for RetransmitTimer<S> {
    type Item = (Instance, ProposerMsg);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<(Instance, ProposerMsg)>, io::Error> {
        match self.msg.as_mut() {
            Some(&mut (inst, ref msg, ref mut f)) => match f.poll()? {
                Async::Ready(Some(())) => Ok(Async::Ready(Some((inst, msg.clone())))),
                Async::Ready(None) => unreachable!("Infinite stream from Scheduler terminated"),
                Async::NotReady => Ok(Async::NotReady),
            },
            None => {
                self.parked_receive = Some(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}

/// Timer that allows the node to re-enter Phase 1 in order to
/// drive resolution with a higher ballot.
struct InstanceResolutionTimer<S: Scheduler> {
    scheduler: S,
    backoff_ms: u64,
    stream: Option<(Instance, S::Stream)>,
    parked_receive: Option<task::Task>,
}

impl<S: Scheduler> InstanceResolutionTimer<S> {
    pub fn new(scheduler: S) -> InstanceResolutionTimer<S> {
        InstanceResolutionTimer {
            scheduler,
            backoff_ms: RESOLUTION_STARTING_MS,
            stream: None,
            parked_receive: None,
        }
    }

    /// Schedules a timer to start Phase 1 when a node receives an ACCEPT
    /// message (Phase 2b) and does not hear an ACCEPTED message from a
    /// quorum of acceptors.
    pub fn schedule_timeout(&mut self, inst: Instance) {
        // TODO: do we want to add some Jitter?
        self.stream = Some((
            inst,
            self.scheduler
                .interval(Duration::from_millis(RESOLUTION_SILENCE_TIMEOUT)),
        ));

        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }

    /// Schedules a timer to schedule retry of the round with a higher ballot.
    pub fn schedule_retry(&mut self, inst: Instance) {
        self.backoff_ms = min(self.backoff_ms * 2, RESOLUTION_MAX_MS);

        let jitter_retry_ms = thread_rng().gen_range(0, self.backoff_ms + 1);
        self.stream = Some((
            inst,
            self.scheduler
                .interval(Duration::from_millis(jitter_retry_ms)),
        ));

        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.backoff_ms = RESOLUTION_STARTING_MS;
        self.stream = None;
    }
}

impl<S: Scheduler> Stream for InstanceResolutionTimer<S> {
    type Item = Instance;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Instance>, io::Error> {
        match self.stream.as_mut() {
            Some(&mut (inst, ref mut f)) => match f.poll()? {
                Async::Ready(Some(())) => Ok(Async::Ready(Some(inst))),
                Async::Ready(None) => unreachable!("Infinite stream from Scheduler terminated"),
                Async::NotReady => Ok(Async::NotReady),
            },
            None => {
                self.parked_receive = Some(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}
