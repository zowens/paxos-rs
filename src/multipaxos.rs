use std::io;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use messages::*;
use algo::*;
use super::Instance;
use state::*;
use config::*;

pub trait ReplicatedState {
    /// Apply a value to the state machine
    fn apply_value(&mut self, instance: Instance, value: Value);

    // TODO: need log semantics
    /// Snapshots the value
    fn snapshot(&self, instance: Instance) -> Option<Value>;
}

pub struct MultiPaxos<R: ReplicatedState> {
    state_machine: R,
    state_handler: StateHandler,

    instance: Instance,
    paxos: PaxosInstance,

    config: Configuration,

    // message that is being sent out for quorum. the
    // retransmission logic will prediodically resend
    // until there is quorum
    retransmit_msg: Option<ProposerMsg>,

    // downstream is sent out from this node
    downstream_sink: UnboundedSender<Message>,
    downstream_stream: UnboundedReceiver<Message>,
}

impl<R: ReplicatedState> MultiPaxos<R> {
    /// Creates a new multi-paxos machine
    pub fn new(mut state_machine: R, config: Configuration) -> MultiPaxos<R> {
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

        MultiPaxos {
            state_machine,
            state_handler,
            instance: state.instance,
            paxos,
            config,
            retransmit_msg: None,
            downstream_sink,
            downstream_stream,
        }
    }

    /// Moves to the next instance with an accepted value
    fn advance_instance(&mut self, instance: Instance, value: Value) {
        self.state_machine.apply_value(instance, value.clone());

        let new_inst = instance + 1;
        info!("Starting instance {}", new_inst);
        self.state_handler.persist(State {
            instance: new_inst,
            current_value: Some(value),
            promised: None,
            accepted: None,
        });
        self.retransmit_msg = None;
        self.paxos =
            PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None);
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
        for peer in peers.into_iter() {
            self.send_multipaxos(MultiPaxosMessage::Prepare(
                self.instance,
                Prepare(peer, prepare.1),
            ));
        }
    }

    /// Broadcasts ACCEPT messages to all peers
    fn send_accept(&mut self, accept: &Accept) {
        let peers = self.config.peers();
        for peer in peers.into_iter() {
            self.send_multipaxos(MultiPaxosMessage::Accept(
                self.instance,
                Accept(peer, accept.1, accept.2.clone()),
            ));
        }
    }

    /// Broadcasts ACCEPTED messages to all peers
    fn send_accepted(&mut self, accepted: &Accepted) {
        let peers = self.config.peers();
        for peer in peers.into_iter() {
            self.send_multipaxos(MultiPaxosMessage::Accepted(
                self.instance,
                Accepted(peer, accepted.1, accepted.2.clone()),
            ));
        }
    }

    fn propose_update(&mut self, value: Value) {
        match self.paxos.propose_value(value) {
            Some(ProposerMsg::Prepare(prepare)) => {
                info!("Starting Phase 1a with proposed value");
                self.send_prepare(&prepare);
                self.retransmit_msg = Some(ProposerMsg::Prepare(prepare));
            }
            Some(ProposerMsg::Accept(accept)) => {
                info!("Starting Phase 2a with proposed value");
                self.send_accept(&accept);
                self.retransmit_msg = Some(ProposerMsg::Accept(accept));
            }
            None => {
                warn!("Alrady have a value during proposal phases");
            }
        }
    }

    #[allow(dead_code)]
    fn poll_retransmit(&mut self, instance: Instance) {
        if instance != self.instance {
            // TODO: cancel
            return;
        }

        // resend prepare messages to peers
        let msg = self.retransmit_msg.take();
        match msg {
            Some(ProposerMsg::Prepare(ref v)) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_prepare(v);
            }
            Some(ProposerMsg::Accept(ref v)) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_accept(v);
            }
            None => {
                // TODO: cancel
            }
        };

        self.retransmit_msg = msg;
    }

    #[allow(dead_code)]
    fn poll_restart_prepare(&mut self, instance: Instance) {
        if instance != self.instance {
            // TODO: cancel
            return;
        }

        let prepare = self.paxos.prepare();
        info!("Restarting Phase 1 with {:?}", prepare.1);
        self.send_prepare(&prepare);
        self.retransmit_msg = Some(ProposerMsg::Prepare(prepare));
    }

    #[allow(dead_code)]
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
            self.retransmit_msg = Some(ProposerMsg::Accept(accept));
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
            self.retransmit_msg = Some(ProposerMsg::Prepare(prepare));
        }
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
            self.advance_instance(inst, current);
        }
    }
}

impl<R: ReplicatedState> Sink for MultiPaxos<R> {
    type SinkItem = Message;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: Message) -> StartSend<Message, io::Error> {
        match msg {
            Message::MultiPaxos(MultiPaxosMessage::Prepare(inst, prepare)) => {
                self.on_prepare(inst, prepare);
            }
            Message::MultiPaxos(MultiPaxosMessage::Promise(inst, promise)) => {
                self.on_promise(inst, promise);
            }
            Message::MultiPaxos(MultiPaxosMessage::Accept(inst, accept)) => {
                self.on_accept(inst, accept);
            }
            Message::MultiPaxos(MultiPaxosMessage::Accepted(inst, accepted)) => {
                self.on_accepted(inst, accepted);
            }
            Message::MultiPaxos(MultiPaxosMessage::Reject(inst, reject)) => {
                self.on_reject(inst, reject);
            }
            Message::MultiPaxos(MultiPaxosMessage::Sync(peer, inst)) => {
                self.on_sync(peer, inst);
            }
            Message::MultiPaxos(MultiPaxosMessage::Catchup(_peer, inst, value)) => {
                self.on_catchup(inst, value);
            }
            Message::Client(ClientMessage::ProposeRequest(_addr, value)) => {
                self.propose_update(value);
            }
            Message::Client(ClientMessage::LookupValueRequest(addr)) => {
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
            Message::Client(_) => {}
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        Ok(Async::Ready(()))
    }
}

impl<R: ReplicatedState> Stream for MultiPaxos<R> {
    type Item = Message;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<Message>, io::Error> {
        self.downstream_stream
            .poll()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Unexpected error"))
    }
}
