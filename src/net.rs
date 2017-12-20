use std::net::SocketAddr;
use std::io;
use tokio_core::net::{UdpCodec, UdpFramed, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use futures::{Future, Sink, Stream};
use futures::stream::SplitStream;
use config::Configuration;
use messenger::{Handler, Messenger};
use messages_capnp as messages;
use super::Instance;
use algo::{Ballot, NodeId, Value};

use capnp::{Error as CapnpError, NotInSchema};
use capnp::message::{Builder, HeapAllocator, Reader, ReaderOptions};
use capnp::serialize::OwnedSegments;
use capnp::serialize_packed::*;

use futures::unsync::mpsc::{unbounded, UnboundedSender};

struct Codec {}

type CodecIn = (SocketAddr, Reader<OwnedSegments>);
type CodecOut = (SocketAddr, Builder<HeapAllocator>);

impl UdpCodec for Codec {
    type In = CodecIn;
    type Out = CodecOut;

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
    sink: UnboundedSender<CodecOut>,
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

#[derive(Debug)]
enum DeserializeError {
    Capnp(CapnpError),
    CapnpNotInSchema(NotInSchema),
}

impl From<CapnpError> for DeserializeError {
    fn from(e: CapnpError) -> DeserializeError {
        DeserializeError::Capnp(e)
    }
}

impl From<NotInSchema> for DeserializeError {
    fn from(e: NotInSchema) -> DeserializeError {
        DeserializeError::CapnpNotInSchema(e)
    }
}

impl Into<io::Error> for DeserializeError {
    fn into(self) -> io::Error {
        error!("Error reading Paxos message from peer: {:?}", self);
        io::Error::new(io::ErrorKind::InvalidData, "Invalid Paxos Message")
    }
}

#[inline]
fn deserialize_ballot<'a>(reader: messages::ballot::Reader<'a>) -> Ballot {
    Ballot(reader.get_id(), reader.get_node_id())
}

fn dispatch<H>(
    reader: Reader<OwnedSegments>,
    mut handler: H,
    peer: NodeId,
) -> Result<(), DeserializeError>
where
    H: Handler,
{
    use messages_capnp::paxos_message::Which as WhichMsg;
    use messages_capnp::paxos_message::promise::Which as WhichLastAccepted;

    let paxos_msg = reader.get_root::<messages::paxos_message::Reader>()?;

    let instance = { paxos_msg.borrow().get_instance() };

    match paxos_msg.which()? {
        WhichMsg::Prepare(prepare) => {
            let prepare = prepare?;
            let proposal = deserialize_ballot(prepare.get_proposal()?);
            handler.on_prepare(peer, instance, proposal);
        }
        WhichMsg::Promise(promise) => {
            let promise = promise?;
            let proposal = { deserialize_ballot(promise.borrow().get_proposal()?) };

            let last_accepted = {
                match promise.which()? {
                    WhichLastAccepted::NoneAccepted(_) => None,
                    WhichLastAccepted::LastAccepted(last_accepted) => {
                        let last_accepted = last_accepted?;
                        let bal = { deserialize_ballot(last_accepted.borrow().get_proposal()?) };
                        let val = last_accepted.get_value()?;
                        Some((bal, Vec::from(val)))
                    }
                }
            };

            handler.on_promise(peer, instance, proposal, last_accepted);
        }
        WhichMsg::Accept(accept) => {
            let accept = accept?;
            let proposal = { deserialize_ballot(accept.borrow().get_proposal()?) };
            let value = accept.get_value()?;
            handler.on_accept(peer, instance, proposal, Vec::from(value));
        }
        WhichMsg::Accepted(accepted) => {
            let accepted = accepted?;
            let proposal = { deserialize_ballot(accepted.borrow().get_proposal()?) };
            let value = accepted.get_value()?;
            handler.on_accepted(peer, instance, proposal, Vec::from(value));
        }
        WhichMsg::Reject(reject) => {
            let reject = reject?;
            let proposal = { deserialize_ballot(reject.borrow().get_proposal()?) };
            let promised = { deserialize_ballot(reject.borrow().get_promised()?) };

            handler.on_reject(peer, instance, proposal, promised);
        }
        WhichMsg::Sync(sync_req) => {
            let _ = sync_req?;
            handler.on_sync(peer, instance);
        }
        WhichMsg::Catchup(catchup) => {
            let catchup = catchup?;
            let value = catchup.get_value()?;
            handler.on_catchup(peer, instance, Vec::from(value));
        }
    }

    Ok(())
}

pub struct UdpServer {
    core: Core,
    config: Configuration,
    send_msg_sink: UnboundedSender<CodecOut>,
    recv_msg_stream: SplitStream<UdpFramed<Codec>>,
}

impl UdpServer {
    pub fn new(config: Configuration) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let (snd, recv) = unbounded::<CodecOut>();
        let (send_msg, recv_msg) = UdpSocket::bind(config.current_address(), &handle)?
            .framed(Codec {})
            .split();

        // forward all messenger messages over UDP
        handle.spawn(
            send_msg
                .send_all(recv.map_err(|_| io::Error::new(io::ErrorKind::Other, "")))
                .map(|_| ())
                .map_err(|e| {
                    error!("Error sending Paxos message: {:?}", e);
                    ()
                }),
        );

        Ok(UdpServer {
            core,
            config,
            send_msg_sink: snd,
            recv_msg_stream: recv_msg,
        })
    }

    pub fn handle(&self) -> Handle {
        self.core.handle()
    }

    pub fn messenger(&self) -> UdpMessenger {
        UdpMessenger {
            sink: self.send_msg_sink.clone(),
            config: self.config.clone(),
        }
    }
}
