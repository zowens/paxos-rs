use std::net::SocketAddr;
use std::io;
use tokio_core::net::{UdpCodec, UdpFramed, UdpSocket};
use tokio_core::reactor::Handle;
use futures::Sink;
use futures::stream::SplitSink;
use config::Configuration;
use messenger::Messenger;
use messages_capnp as messages;
use super::Instance;
use algo::{Ballot, NodeId, Value};

use capnp::message::{Builder, HeapAllocator, Reader, ReaderOptions};
use capnp::serialize::OwnedSegments;
use capnp::serialize_packed::*;

use futures::unsync::mpsc::UnboundedSender;

struct Codec {}

impl UdpCodec for Codec {
    type In = (SocketAddr, Reader<OwnedSegments>);
    type Out = (SocketAddr, Builder<HeapAllocator>);

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<Self::In> {
        let mut cursor = io::Cursor::new(buf);
        read_message(&mut cursor, ReaderOptions::new())
            .map(|r| (*addr, r))
            .map_err(|e| {
                error!("Error deserializing Paxos Message: {:?}", e);
                io::Error::new(io::ErrorKind::InvalidData, "Invalid Paxos Message")
            })
    }

    fn encode(&mut self, (addr, msg): Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        if let Err(e) = write_message(into, &msg) {
            error!("Error serializing Paxos Message: {:?}", e);
        }

        addr
    }
}


pub struct UdpMessenger {
    sink: UnboundedSender<(SocketAddr, Builder<HeapAllocator>)>,
    config: Configuration,
}

impl UdpMessenger {
    fn send(&self, peer: NodeId, builder: Builder<HeapAllocator>) {
        match self.config.address(peer) {
            Some(addr) => {
                if let Err(e) = self.sink.unbounded_send((addr, builder)) {
                    error!("Unable to send message: {:?}", e);
                }
            }
            None => {
                warn!("No socket address found for NodeId={}", peer);
            }
        }
    }
}

#[inline]
fn serialize_ballot<'a>(proposal: Ballot, mut builder: messages::ballot::Builder<'a>) {
    builder.set_id(proposal.0);
    builder.set_node_id(proposal.1);
}

impl Messenger for UdpMessenger {
    /// PREPARE message (phase 1a) messages propose a new ballot
    fn send_prepare(&self, peer: NodeId, inst: Instance, proposal: Ballot) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);

            let prepare = msg.borrow().init_prepare();
            let ballot = prepare.init_proposal();
            serialize_ballot(proposal, ballot);
        }

        self.send(peer, builder);
    }

    /// PROMISE message (phase 1b) from acceptors promise to not accept ballots
    /// that preceed the proposal from the proposer.
    fn send_promise(
        &self,
        peer: NodeId,
        inst: Instance,
        proposal: Ballot,
        last_accepted: Option<(Ballot, Value)>,
    ) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);

            let mut promise = msg.borrow().init_promise();

            {
                let ballot = promise.borrow().init_proposal();
                serialize_ballot(proposal, ballot);
            }

            match last_accepted {
                Some((bal, val)) => {
                    let mut last_accepted = promise.borrow().init_last_accepted();
                    last_accepted.set_value(&val);
                    let last_accepted_proposal = last_accepted.init_proposal();
                    serialize_ballot(bal, last_accepted_proposal);
                }
                None => {
                    promise.set_none_accepted(());
                }
            }
        }

        self.send(peer, builder);
    }

    /// ACCEPT messages (phase 2a) from proposers to acceptors broadcast a value to accept
    /// for an instance.
    fn send_accept(&self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);

            let mut accept = msg.borrow().init_accept();

            {
                let ballot = accept.borrow().init_proposal();
                serialize_ballot(proposal, ballot);
            }

            accept.set_value(&value);
        }

        self.send(peer, builder);
    }

    /// ACCEPTED messages (phase 2b) from acceptors is sent to learners that a value for
    /// an instance has been accepted.
    fn send_accepted(&self, peer: NodeId, inst: Instance, proposal: Ballot, value: Value) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);

            let mut accepted = msg.borrow().init_accepted();

            {
                let ballot = accepted.borrow().init_proposal();
                serialize_ballot(proposal, ballot);
            }

            accepted.set_value(&value);
        }

        self.send(peer, builder);
    }

    /// Reject messages (also referred to as NACK) reject a promise or accept message from an
    /// acceptor that has already promise a ballot of that is > the proposal from the peer.
    /// This speeds up consensus rather than relying on retries exclusively.
    fn send_reject(&self, peer: NodeId, inst: Instance, proposal: Ballot, promised: Ballot) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);

            let mut reject = msg.borrow().init_reject();

            {
                let ballot = reject.borrow().init_proposal();
                serialize_ballot(proposal, ballot);
            }

            {
                let ballot = reject.borrow().init_promised();
                serialize_ballot(promised, ballot);
            }
        }

        self.send(peer, builder);
    }

    /// Sync will request the value from an instance
    fn send_sync(&self, peer: NodeId, inst: Instance) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);
            msg.borrow().init_sync();
        }

        self.send(peer, builder);
    }

    /// Catchup will send a caught up value from an instance.
    fn send_catchup(&self, peer: NodeId, inst: Instance, current: Value) {
        let mut builder = Builder::new(HeapAllocator::new());

        {
            let mut msg = builder.init_root::<messages::paxos_message::Builder>();
            msg.set_instance(inst);
            let mut catchup = msg.borrow().init_catchup();
            catchup.set_value(&current);
        }

        self.send(peer, builder);
    }
}
