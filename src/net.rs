use bytes::{BufMut, BytesMut};
use config::Configuration;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use messages::{ClusterMessage, MultiPaxosMessage};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::de;
use serde_cbor::ser;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use tokio::net::{UdpFramed, UdpSocket};
use tokio::runtime::Runtime;
use tokio_io::codec::{Decoder, Encoder};

#[derive(Default)]
struct MultiPaxosCodec<V: DeserializeOwned + Serialize>(PhantomData<V>);

impl<V: DeserializeOwned + Serialize> Decoder for MultiPaxosCodec<V> {
    type Item = MultiPaxosMessage<V>;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match de::from_slice::<MultiPaxosMessage<V>>(src) {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => {
                error!("Invalid CBOR data sent: {}", e);
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid CBOR request",
                ))
            }
        }
    }
}

impl<V: DeserializeOwned + Serialize> Encoder for MultiPaxosCodec<V> {
    type Item = MultiPaxosMessage<V>;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut w = dst.writer();
        match ser::to_writer_packed(&mut w, &item) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Error serializing message: {}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unable to serialize message",
                ))
            }
        }
    }
}

/// Multi-paxos node that receives and sends nodes over a network.
pub struct NetworkedMultiPaxos<V, S>
where
    S: Stream<Item = ClusterMessage<V>, Error = io::Error>,
    S: Sink<SinkItem = ClusterMessage<V>, SinkError = io::Error>,
{
    s: S,
    config: Configuration,
}

impl<V, S> Sink for NetworkedMultiPaxos<V, S>
where
    S: Stream<Item = ClusterMessage<V>, Error = io::Error>,
    S: Sink<SinkItem = ClusterMessage<V>, SinkError = io::Error>,
{
    type SinkItem = (MultiPaxosMessage<V>, SocketAddr);
    type SinkError = io::Error;

    fn start_send(&mut self, msg: Self::SinkItem) -> StartSend<Self::SinkItem, io::Error> {
        let (message, address) = msg;
        let peer = match self.config.peer_id(&address) {
            Some(v) => v,
            None => {
                warn!(
                    "Received message from address, but is not in configuration: {}",
                    address
                );
                return Ok(AsyncSink::Ready);
            }
        };

        let send_res = self.s.start_send(ClusterMessage { peer, message })?;
        match send_res {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(ClusterMessage { message, .. }) => {
                Ok(AsyncSink::NotReady((message, address)))
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.s.poll_complete()
    }
}

impl<V, S> Stream for NetworkedMultiPaxos<V, S>
where
    S: Stream<Item = ClusterMessage<V>, Error = io::Error>,
    S: Sink<SinkItem = ClusterMessage<V>, SinkError = io::Error>,
{
    type Item = (MultiPaxosMessage<V>, SocketAddr);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match try_ready!(self.s.poll()) {
                Some(ClusterMessage { peer, message }) => {
                    if let Some(address) = self.config.address(peer) {
                        return Ok(Async::Ready(Some((message, address))));
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

/// Server that runs a multi-paxos node over the network with UDP.
pub struct UdpServer {
    socket: UdpSocket,
    config: Configuration,
    runtime: Runtime,
}

impl UdpServer {
    /// Creates a new `UdpServer` with the address of the node
    /// specified in the configuration.
    pub fn new(config: Configuration) -> io::Result<UdpServer> {
        let runtime = Runtime::new()?;
        let socket = UdpSocket::bind(config.current_address())?;

        Ok(UdpServer {
            socket,
            config,
            runtime,
        })
    }

    /// Gets a handle in order to spawn additional futures other than the multi-paxos
    /// node. For example, a client-facing protocol can be spawned with a handle.
    pub fn runtime_mut(&mut self) -> &mut Runtime {
        &mut self.runtime
    }

    /// Runs a multi-paxos node, blocking until the program is terminated.
    pub fn run<S: Send + 'static, V: Send + DeserializeOwned + Serialize + 'static>(
        mut self,
        multipaxos: S,
    ) where
        S: Stream<Item = ClusterMessage<V>, Error = io::Error>,
        S: Sink<SinkItem = ClusterMessage<V>, SinkError = io::Error>,
    {
        let multipaxos = NetworkedMultiPaxos {
            s: multipaxos,
            config: self.config,
        };
        let (sink, stream) = multipaxos.split();

        let codec: MultiPaxosCodec<V> = MultiPaxosCodec(PhantomData);
        let (net_sink, net_stream) = UdpFramed::new(self.socket, codec).split();

        // send replies from upstream to the network
        self.runtime.spawn(EmptyFuture(stream.forward(net_sink)));
        // receive messages from network to upstream
        self.runtime.spawn(EmptyFuture(net_stream.forward(sink)));

        self.runtime.shutdown_on_idle().wait().unwrap();
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
