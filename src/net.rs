use std::net::SocketAddr;
use std::io;
use std::fmt;
use tokio_core::net::{UdpCodec, UdpFramed, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use futures::{Async, Future, Poll, Sink, Stream};
use config::Configuration;
use messages::MultiPaxosMessage;

struct Codec(Configuration);

impl UdpCodec for Codec {
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
            None => {
                panic!("No peer address for NodeId={}", peer);
            }
        };

        if let Err(e) = msg.serialize(into) {
            error!("Error serialize message: {}", e);
        }

        addr
    }
}

pub struct UdpServer {
    core: Core,
    framed: UdpFramed<Codec>,
}

impl UdpServer {
    pub fn new(config: Configuration) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let framed =
            UdpSocket::bind(config.current_address(), &handle)?.framed(Codec(config.clone()));


        Ok(UdpServer { core, framed })
    }

    pub fn handle(&self) -> Handle {
        self.core.handle()
    }


    pub fn run<S: 'static>(mut self, upstream: S) -> Result<(), ()>
    where
        S: Stream<Item = MultiPaxosMessage, Error = io::Error>,
        S: Sink<SinkItem = MultiPaxosMessage, SinkError = io::Error>,
    {
        let (sink, stream) = upstream.split();
        let (out_sink, recv_stream) = self.framed.split();
        self.core
            .handle()
            .spawn(EmptyFuture(stream.forward(out_sink)));
        self.core
            .run(EmptyFuture(recv_stream.filter_map(|v| v).forward(sink)))
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
