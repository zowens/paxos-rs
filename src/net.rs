use std::net::SocketAddr;
use std::io;
use std::fmt;
use tokio_core::net::{UdpCodec, UdpFramed, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use messages::{ClientMessage, Message, MultiPaxosMessage, NetworkMessage};

#[derive(Default)]
struct MultiPaxosCodec;

impl UdpCodec for MultiPaxosCodec {
    type In = Option<NetworkMessage<MultiPaxosMessage>>;
    type Out = NetworkMessage<MultiPaxosMessage>;

    fn decode(
        &mut self,
        addr: &SocketAddr,
        buf: &[u8],
    ) -> io::Result<Option<NetworkMessage<MultiPaxosMessage>>> {
        Ok(MultiPaxosMessage::deserialize(buf)
            .map_err(|e| error!("Error deserializing message {:?}", e))
            .map(|m| {
                NetworkMessage {
                    address: *addr,
                    message: m,
                }
            })
            .ok())
    }

    fn encode(&mut self, out: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        let NetworkMessage { address, message } = out;
        if let Err(e) = message.serialize(into) {
            error!("Error serialize message: {}", e);
        }

        address
    }
}

#[derive(Default)]
struct ClientMessageCodec;

impl UdpCodec for ClientMessageCodec {
    type In = Option<NetworkMessage<ClientMessage>>;
    type Out = NetworkMessage<ClientMessage>;

    fn decode(
        &mut self,
        addr: &SocketAddr,
        buf: &[u8],
    ) -> io::Result<Option<NetworkMessage<ClientMessage>>> {
        Ok(ClientMessage::deserialize(buf)
            .map_err(|e| error!("Error deserializing message {:?}", e))
            .ok()
            .map(|m| {
                NetworkMessage {
                    address: *addr,
                    message: m,
                }
            }))
    }

    fn encode(&mut self, out: Self::Out, into: &mut Vec<u8>) -> SocketAddr {
        let NetworkMessage { address, message } = out;
        message.serialize(into).unwrap();
        address
    }
}

/// Client to the UDP server.
pub struct UdpClient {
    client_framed: UdpFramed<ClientMessageCodec>,
}

impl UdpClient {
    pub fn new(addr: &SocketAddr, handle: &Handle) -> io::Result<UdpClient> {
        Ok(UdpClient {
            client_framed: UdpSocket::bind(addr, handle)?.framed(ClientMessageCodec),
        })
    }
}

impl Sink for UdpClient {
    type SinkItem = NetworkMessage<ClientMessage>;
    type SinkError = io::Error;

    fn start_send(
        &mut self,
        msg: NetworkMessage<ClientMessage>,
    ) -> StartSend<NetworkMessage<ClientMessage>, io::Error> {
        self.client_framed.start_send(msg)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.client_framed.poll_complete()
    }
}

impl Stream for UdpClient {
    type Item = NetworkMessage<ClientMessage>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<NetworkMessage<ClientMessage>>, io::Error> {
        // filter out values that are None (not parsed)
        loop {
            match try_ready!(self.client_framed.poll()) {
                Some(Some(e)) => return Ok(Async::Ready(Some(e))),
                Some(None) => {}
                None => return Ok(Async::Ready(None)),
            }
        }
    }
}

/// Server that runs multi-paxos.
pub struct UdpServer {
    core: Core,
    framed: UdpFramed<MultiPaxosCodec>,
    client_framed: UdpFramed<ClientMessageCodec>,
}

impl UdpServer {
    pub fn new(multi_paxos_addr: &SocketAddr, client_addr: &SocketAddr) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let framed = UdpSocket::bind(multi_paxos_addr, &handle)?.framed(MultiPaxosCodec);

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
        S: Stream<Item = NetworkMessage<Message>, Error = io::Error>,
        S: Sink<SinkItem = NetworkMessage<Message>, SinkError = io::Error>,
    {
        let (sink, stream) = upstream.split();

        let (paxos_sink, paxos_recv_stream) = self.framed.split();
        let (client_sink, client_recv_stream) = self.client_framed.split();

        let paxos_recv_stream = paxos_recv_stream.filter_map(|v| {
            v.map(|m| {
                NetworkMessage {
                    address: m.address,
                    message: Message::MultiPaxos(m.message),
                }
            })
        });
        let client_recv_stream = client_recv_stream.filter_map(|v| {
            v.map(|m| {
                NetworkMessage {
                    address: m.address,
                    message: Message::Client(m.message),
                }
            })
        });

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

    buffered: Option<NetworkMessage<Message>>,
}

impl<S, P, C> ForwardMessage<S, P, C>
where
    S: Stream<Item = NetworkMessage<Message>, Error = io::Error>,
    P: Sink<SinkItem = NetworkMessage<MultiPaxosMessage>, SinkError = io::Error>,
    C: Sink<SinkItem = NetworkMessage<ClientMessage>, SinkError = io::Error>,
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

    fn try_start_send(&mut self, item: NetworkMessage<Message>) -> Poll<(), io::Error> {
        match item {
            NetworkMessage {
                address,
                message: Message::MultiPaxos(m),
            } => {
                let m = NetworkMessage {
                    address,
                    message: m,
                };
                if let AsyncSink::NotReady(item) = self.paxos_sink.start_send(m)? {
                    self.buffered = Some(NetworkMessage {
                        address: item.address,
                        message: Message::MultiPaxos(item.message),
                    });
                    return Ok(Async::NotReady);
                } else {
                    self.paxos_poll_complete = true;
                }
            }
            NetworkMessage {
                address,
                message: Message::Client(m),
            } => {
                let m = NetworkMessage {
                    address,
                    message: m,
                };
                if let AsyncSink::NotReady(item) = self.client_sink.start_send(m)? {
                    self.buffered = Some(NetworkMessage {
                        address: item.address,
                        message: Message::Client(item.message),
                    });
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
    S: Stream<Item = NetworkMessage<Message>, Error = io::Error>,
    P: Sink<SinkItem = NetworkMessage<MultiPaxosMessage>, SinkError = io::Error>,
    C: Sink<SinkItem = NetworkMessage<ClientMessage>, SinkError = io::Error>,
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
