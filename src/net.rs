use std::net::SocketAddr;
use std::io;
use std::fmt;
use std::marker::PhantomData;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::de;
use serde_cbor::ser;
use tokio_core::net::{UdpCodec, UdpSocket};
use tokio_core::reactor::{Core, Handle};
use messages::{ClusterMessage, MultiPaxosMessage, NetworkMessage};
use config::Configuration;

#[derive(Default)]
struct MultiPaxosCodec<V: DeserializeOwned + Serialize>(PhantomData<V>);

impl<V: DeserializeOwned + Serialize> UdpCodec for MultiPaxosCodec<V> {
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
    type SinkItem = NetworkMessage<V>;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: Self::SinkItem) -> StartSend<Self::SinkItem, io::Error> {
        let NetworkMessage { address, message } = msg;
        let peer = match self.config.peer_id(&address) {
            Some(v) => v,
            None => {
                warn!(
                    "Received message from address, but is not in configuration: {}",
                    msg.address
                );
                return Ok(AsyncSink::Ready);
            }
        };

        let send_res = self.s.start_send(ClusterMessage { peer, message })?;
        match send_res {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(ClusterMessage { message, .. }) => {
                Ok(AsyncSink::NotReady(NetworkMessage { address, message }))
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
    type Item = NetworkMessage<V>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<NetworkMessage<V>>, io::Error> {
        loop {
            match try_ready!(self.s.poll()) {
                Some(ClusterMessage { peer, message }) => {
                    if let Some(address) = self.config.address(peer) {
                        return Ok(Async::Ready(Some(NetworkMessage { address, message })));
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
    core: Core,
    socket: UdpSocket,
    config: Configuration,
}

impl UdpServer {
    /// Creates a new `UdpServer` with the address of the node
    /// specified in the configuration.
    pub fn new(config: Configuration) -> io::Result<UdpServer> {
        let core = Core::new()?;
        let handle = core.handle();

        let socket = UdpSocket::bind(config.current_address(), &handle)?;

        Ok(UdpServer {
            core,
            socket,
            config,
        })
    }

    /// Gets a handle in order to spawn additional futures other than the multi-paxos
    /// node. For example, a client-facing protocol can be spawned with a handle.
    pub fn handle(&self) -> Handle {
        self.core.handle()
    }

    /// Runs a multi-paxos node, blocking until the program is terminated.
    pub fn run<S: 'static, V: DeserializeOwned + Serialize + 'static>(
        mut self,
        multipaxos: S,
    ) -> Result<(), ()>
    where
        S: Stream<Item = ClusterMessage<V>, Error = io::Error>,
        S: Sink<SinkItem = ClusterMessage<V>, SinkError = io::Error>,
    {
        let multipaxos = NetworkedMultiPaxos {
            s: multipaxos,
            config: self.config,
        };
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
