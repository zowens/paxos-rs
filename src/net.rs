use std::net::SocketAddr;
use std::io;
use std::fmt;
use tokio_core::net::{UdpCodec, UdpFramed, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use config::Configuration;
use messages::{ClientMessage, Message, MultiPaxosMessage};

struct MultiPaxosCodec(Configuration);

impl UdpCodec for MultiPaxosCodec {
    type In = Option<MultiPaxosMessage>;
    type Out = MultiPaxosMessage;

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<Option<MultiPaxosMessage>> {
        match self.0.peer_id(addr) {
            Some(peer) => Ok(MultiPaxosMessage::deserialize(peer, buf)
                .map_err(|e| error!("Error deserializing message {:?}", e))
                .ok()),
            None => {
                warn!("No such peer at {:?}", addr);
                Ok(None)
            }
        }
    }

    fn encode(&mut self, msg: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        let peer = msg.peer();
        let addr = match self.0.address(msg.peer()) {
            Some(addr) => addr,
            None => panic!("No peer address for NodeId={}", peer),
        };

        if let Err(e) = msg.serialize(into) {
            error!("Error serialize message: {}", e);
        }

        addr
    }
}

#[derive(Default)]
struct ClientMessageCodec;

impl UdpCodec for ClientMessageCodec {
    type In = Option<ClientMessage>;
    type Out = ClientMessage;

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<Option<ClientMessage>> {
        Ok(ClientMessage::deserialize(*addr, buf)
            .map_err(|e| error!("Error deserializing message {:?}", e))
            .ok()
            .filter(|v| match *v {
                ClientMessage::ProposeRequest(..) => true,
                ClientMessage::LookupValueRequest(..) => true,
                _ => {
                    warn!("Ignoring non-request message");
                    false
                }
            }))
    }

    fn encode(&mut self, msg: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        msg.serialize(into).unwrap()
    }
}

#[derive(Default)]
struct ClientCodec;

impl UdpCodec for ClientCodec {
    type In = ClientMessage;
    type Out = ClientMessage;

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<ClientMessage> {
        ClientMessage::deserialize(*addr, buf).map_err(|e| {
            error!("Error deserializing message {:?}", e);
            e.into()
        })
    }

    fn encode(&mut self, msg: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        msg.serialize(into).unwrap()
    }
}

pub struct UdpClient {
    client_framed: UdpFramed<ClientCodec>,
}

impl UdpClient {
    pub fn new(addr: &SocketAddr, handle: &Handle) -> io::Result<UdpClient> {
        Ok(UdpClient {
            client_framed: UdpSocket::bind(addr, handle)?.framed(ClientCodec),
        })
    }
}

impl Sink for UdpClient {
    type SinkItem = ClientMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: ClientMessage) -> StartSend<ClientMessage, io::Error> {
        self.client_framed.start_send(msg)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.client_framed.poll_complete()
    }
}

impl Stream for UdpClient {
    type Item = ClientMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<ClientMessage>, io::Error> {
        self.client_framed.poll()
    }
}

/// Server that runs multi-paxos.
pub struct UdpServer {
    core: Core,
    framed: UdpFramed<MultiPaxosCodec>,
    client_framed: UdpFramed<ClientMessageCodec>,
}

impl UdpServer {
    pub fn new(config: Configuration, client_addr: &SocketAddr) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let framed =
            UdpSocket::bind(config.current_address(), &handle)?.framed(MultiPaxosCodec(config));

        let client_framed = UdpSocket::bind(client_addr, &handle)?.framed(ClientMessageCodec);

        Ok(UdpServer {
            core,
            framed,
            client_framed,
        })
    }

    pub fn handle(&self) -> Handle {
        self.core.handle()
    }

    pub fn run<S: 'static>(mut self, upstream: S) -> Result<(), ()>
    where
        S: Stream<Item = Message, Error = io::Error>,
        S: Sink<SinkItem = Message, SinkError = io::Error>,
    {
        let (sink, stream) = upstream.split();

        let (paxos_sink, paxos_recv_stream) = self.framed.split();
        let (client_sink, client_recv_stream) = self.client_framed.split();

        let paxos_recv_stream = paxos_recv_stream.filter_map(|v| v.map(Message::MultiPaxos));
        let client_recv_stream = client_recv_stream.filter_map(|m| m.map(Message::Client));

        // send replies from upstream to the network
        self.core.handle().spawn(EmptyFuture(ForwardMessage::new(
            stream,
            paxos_sink,
            client_sink,
        )));

        // receive messages from network to upstream
        self.core.run(EmptyFuture(
            paxos_recv_stream.select(client_recv_stream).forward(sink),
        ))
    }
}

struct EmptyFuture<F>(F);

impl<V, E, S> Future for EmptyFuture<S>
where
    S: Future<Item = V, Error = E>,
    E: fmt::Display,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        match self.0.poll() {
            Ok(Async::Ready(_)) => Ok(Async::Ready(())),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                error!("{}", e);
                Err(())
            }
        }
    }
}

struct ForwardMessage<S, P, C> {
    stream: S,

    paxos_sink: P,
    client_sink: C,

    paxos_poll_complete: bool,
    client_poll_complete: bool,

    buffered: Option<Message>,
}

impl<S, P, C> ForwardMessage<S, P, C>
where
    S: Stream<Item = Message, Error = io::Error>,
    P: Sink<SinkItem = MultiPaxosMessage, SinkError = io::Error>,
    C: Sink<SinkItem = ClientMessage, SinkError = io::Error>,
{
    fn new(stream: S, paxos_sink: P, client_sink: C) -> ForwardMessage<S, P, C> {
        ForwardMessage {
            stream,
            paxos_sink,
            client_sink,
            paxos_poll_complete: false,
            client_poll_complete: false,
            buffered: None,
        }
    }

    fn try_start_send(&mut self, item: Message) -> Poll<(), io::Error> {
        match item {
            Message::MultiPaxos(m) => {
                if let AsyncSink::NotReady(item) = self.paxos_sink.start_send(m)? {
                    self.buffered = Some(Message::MultiPaxos(item));
                    return Ok(Async::NotReady);
                } else {
                    self.paxos_poll_complete = true;
                }
            }
            Message::Client(m) => {
                if let AsyncSink::NotReady(item) = self.client_sink.start_send(m)? {
                    self.buffered = Some(Message::Client(item));
                    return Ok(Async::NotReady);
                } else {
                    self.client_poll_complete = true;
                }
            }
        }
        Ok(Async::Ready(()))
    }

    fn try_poll_complete(&mut self) -> Poll<(), io::Error> {
        if self.paxos_poll_complete {
            try_ready!(self.paxos_sink.poll_complete());
            self.paxos_poll_complete = false;
        }
        if self.client_poll_complete {
            try_ready!(self.client_sink.poll_complete());
            self.client_poll_complete = false;
        }
        Ok(Async::Ready(()))
    }
}

impl<S, P, C> Future for ForwardMessage<S, P, C>
where
    S: Stream<Item = Message, Error = io::Error>,
    P: Sink<SinkItem = MultiPaxosMessage, SinkError = io::Error>,
    C: Sink<SinkItem = ClientMessage, SinkError = io::Error>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        // If we've got an item buffered already, we need to write it to the
        // sink before we can do anything else
        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item))
        }

        loop {
            match self.stream.poll()? {
                Async::Ready(Some(item)) => try_ready!(self.try_start_send(item)),
                Async::Ready(None) => {
                    try_ready!(self.paxos_sink.close());
                    try_ready!(self.client_sink.close());
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    try_ready!(self.try_poll_complete());
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}
