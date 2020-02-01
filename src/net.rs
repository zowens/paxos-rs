use bytes::BytesMut;
use futures::TryFutureExt;
use bytes::buf::BufMutExt;
use crate::config::Configuration;
use futures::{Sink, Stream};
use crate::messages::{ClusterMessage, MultiPaxosMessage};
use serde::Serialize;
use serde_cbor::{de, ser};
use std::io;
use std::net::SocketAddr;
use tokio_util::udp::UdpFramed;
use tokio::runtime::Runtime;
use tokio_util::codec::{Decoder, Encoder};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::UdpSocket;
use std::future::Future;
use futures::stream::StreamExt;

#[derive(Default)]
struct MultiPaxosCodec;

impl Decoder for MultiPaxosCodec {
    type Item = MultiPaxosMessage;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match de::from_slice::<MultiPaxosMessage>(src) {
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

impl Encoder for MultiPaxosCodec {
    type Item = MultiPaxosMessage;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let w = dst.writer();
        match item.serialize(&mut ser::Serializer::new(&mut ser::IoWrite::new(w)).packed_format()) {
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
#[pin_project]
pub struct NetworkedMultiPaxos<S>
where
    S: Stream<Item = ClusterMessage>,
    S: Sink<ClusterMessage, Error = io::Error>,
{
    #[pin]
    s: S,
    config: Configuration,
}

impl<S> Sink<(MultiPaxosMessage, SocketAddr)> for NetworkedMultiPaxos<S>
where
    S: Stream<Item = ClusterMessage>,
    S: Sink<ClusterMessage, Error = io::Error>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().s.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: (MultiPaxosMessage, SocketAddr)) -> Result<(), Self::Error> {
        let (message, address) = item;
        let peer = match self.config.peer_id(&address) {
            Some(v) => v,
            None => {
                warn!(
                    "Received message from address, but is not in configuration: {}",
                    address
                );
                return Ok(());
            }
        };

        self.project().s.start_send(ClusterMessage { peer, message })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().s.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().s.poll_close(cx)
    }
}

impl<S> Stream for NetworkedMultiPaxos<S>
where
    S: Stream<Item = ClusterMessage>,
    S: Sink<ClusterMessage, Error = io::Error>,
{
    type Item = (MultiPaxosMessage, SocketAddr);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let self_pin = self.as_mut().project();
            match ready!(self_pin.s.poll_next(cx)) {
                Some(ClusterMessage { peer, message }) => {
                    if let Some(address) = self.config.address(peer) {
                        return Poll::Ready(Some((message, address)));
                    } else {
                        warn!("Unknown peer {:?}", peer);
                    }
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Server that runs a multi-paxos node over the network with UDP.
pub struct UdpServer {
    config: Configuration,
    runtime: Runtime,
}

impl UdpServer {
    /// Creates a new `UdpServer` with the address of the node
    /// specified in the configuration.
    pub fn new(config: Configuration) -> io::Result<UdpServer> {
        let runtime = Runtime::new()?;

        Ok(UdpServer {
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
    pub fn run<S: Send + 'static>(
        self,
        multipaxos: S,
    ) where
        S: Stream<Item = ClusterMessage>,
        S: Sink<ClusterMessage, Error = io::Error>,
    {
        let UdpServer { config, mut runtime } = self;
        let current_address = config.current_address().clone();
        let f = UdpSocket::bind(current_address).and_then(move |socket| {
            let multipaxos = NetworkedMultiPaxos {
                s: multipaxos,
                config: config,
            };
            let (sink, stream) = multipaxos.split();

            let codec: MultiPaxosCodec = MultiPaxosCodec;
            let (net_sink, net_stream) = UdpFramed::new(socket, codec).split();

            // send replies from upstream to the network
            tokio::spawn(EmptyFuture(stream.map(Ok).forward(net_sink)));

            // receive messages from network to upstream
            EmptyFuture(net_stream.forward(sink))
        });
        runtime.block_on(f).expect("Expected no errors when polling UDP socket");
    }
}

#[pin_project]
struct EmptyFuture<F>(#[pin] F);

impl<V, S> Future for EmptyFuture<S>
where
    S: Future<Output = V>,
{
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project().0.poll(cx) {
            Poll::Ready(_) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending
        }
    }
}
