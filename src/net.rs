use std::net::SocketAddr;
use std::io;
use std::fmt;
use std::marker::PhantomData;
use futures::{Async, Future, Poll, Sink, Stream};
use serde_cbor::de;
use serde_cbor::ser;
use tokio_core::net::{UdpCodec, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use algo::Value;
use messages::{MultiPaxosMessage, NetworkMessage};
use config::Configuration;

#[derive(Default)]
struct MultiPaxosCodec<V: Value + 'static>(PhantomData<V>);

impl<V: Value + 'static> UdpCodec for MultiPaxosCodec<V> {
    type In = Option<NetworkMessage<V>>;
    type Out = NetworkMessage<V>;

    fn decode(&mut self, addr: &SocketAddr, buf: &[u8]) -> io::Result<Option<NetworkMessage<V>>> {
        Ok(de::from_slice::<MultiPaxosMessage<V>>(buf)
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
        if let Err(e) = ser::to_writer_packed(into, &message) {
            error!("Error serialize message: {}", e);
        }

        address
    }
}

/// Server that runs a multi-paxos node over the network with UDP.
pub struct UdpServer {
    core: Core,
    socket: UdpSocket,
}

impl UdpServer {
    /// Creates a new `UdpServer` with the address of the node
    /// specified in the configuration.
    pub fn new(config: &Configuration) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let socket = UdpSocket::bind(config.current_address(), &handle)?;

        Ok(UdpServer { core, socket })
    }

    /// Gets a handle in order to spawn additional futures other than the multi-paxos
    /// node. For example, a client-facing protocol can be spawned with a handle.
    pub fn handle(&self) -> Handle {
        self.core.handle()
    }

    /// Runs a multi-paxos node, blocking until the program is terminated.
    pub fn run<S: 'static, V: Value + 'static>(mut self, multipaxos: S) -> Result<(), ()>
    where
        S: Stream<Item = NetworkMessage<V>, Error = io::Error>,
        S: Sink<SinkItem = NetworkMessage<V>, SinkError = io::Error>,
    {
        let (sink, stream) = multipaxos.split();

        let codec: MultiPaxosCodec<V> = MultiPaxosCodec(PhantomData);
        let (net_sink, net_stream) = self.socket.framed(codec).split();

        let net_stream = net_stream.filter_map(|v| v);

        // send replies from upstream to the network
        self.core
            .handle()
            .spawn(EmptyFuture(stream.forward(net_sink)));

        // receive messages from network to upstream
        self.core.run(EmptyFuture(net_stream.forward(sink)))
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
