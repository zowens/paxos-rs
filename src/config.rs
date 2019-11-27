use rand::prelude::{thread_rng, SmallRng, Rng, SeedableRng};
use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

/// A `NodeId` is a unique value that identifies a node
/// within the configuration.
pub type NodeId = u32;

/// Configuration holds the state of the membership of the cluster.
/// TODO: add reconfiguration
#[derive(Clone)]
pub struct Configuration {
    current: (NodeId, SocketAddr),
    peers: HashMap<NodeId, SocketAddr>,
    socket_to_peer: HashMap<SocketAddr, NodeId>,
    rand: SmallRng,
}

impl Configuration {
    /// Creates a new configuration
    pub fn new<I>(current: (NodeId, SocketAddr), peers: I) -> Configuration
    where
        I: Iterator<Item = (NodeId, SocketAddr)>,
    {
        let peers: HashMap<NodeId, SocketAddr> = peers.collect();
        let socket_to_peer: HashMap<SocketAddr, NodeId> =
            peers.iter().map(|e| (*e.1, *e.0)).collect();
        Configuration {
            current,
            peers,
            socket_to_peer,
            rand: SmallRng::from_rng(thread_rng()).unwrap(),
        }
    }

    /// Size of quorum
    pub fn quorum_size(&self) -> usize {
        1 + (self.peers.len() / 2)
    }

    /// Current node identifier
    pub fn current(&self) -> NodeId {
        self.current.0
    }

    /// Current node address
    pub fn current_address(&self) -> &SocketAddr {
        &self.current.1
    }

    /// Iterator containing `NodeId` values of peers
    pub fn peers(&self) -> PeerIntoIter {
        PeerIntoIter { r: &self }
    }

    /// Randomly selects a peer to transmit messages.
    pub fn random_peer(&mut self) -> Option<NodeId> {
        let len = self.peers.len();
        let n = self.rand.gen_range(0, len);
        self.peers.keys().nth(n).cloned()
    }

    /// Gets the address of a node.
    pub fn address(&self, node: NodeId) -> Option<SocketAddr> {
        if node == self.current.0 {
            Some(self.current.1)
        } else {
            self.peers.get(&node).cloned()
        }
    }

    /// Gets the peer ID from a socket address.
    pub fn peer_id(&self, address: &SocketAddr) -> Option<NodeId> {
        if address == &self.current.1 {
            Some(self.current.0)
        } else {
            self.socket_to_peer.get(address).cloned()
        }
    }
}

impl fmt::Debug for Configuration {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let quorum_size = self.quorum_size();
        fmt.debug_struct("Configuration")
            .field("current_node_id", &self.current.0)
            .field("current_node_address", &self.current.1)
            .field("peers", &self.peers)
            .field("peers_to_socket", &self.socket_to_peer)
            .field("quorum", &quorum_size)
            .finish()
    }
}

/// `IntoIterator` for peer node identifiers
pub struct PeerIntoIter<'a> {
    r: &'a Configuration,
}

impl<'a, 'b: 'a> IntoIterator for &'b PeerIntoIter<'a> {
    type IntoIter = PeerIter<'a>;
    type Item = NodeId;

    fn into_iter(self) -> PeerIter<'a> {
        PeerIter {
            iter: self.r.peers.keys(),
        }
    }
}

/// `Iterator` for the peer node identifiers
pub struct PeerIter<'a> {
    iter: hash_map::Keys<'a, NodeId, SocketAddr>,
}

impl<'a> Iterator for PeerIter<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().cloned()
    }
}
