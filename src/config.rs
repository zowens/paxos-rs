use std::collections::HashMap;
use std::collections::hash_map;
use std::net::SocketAddr;
use algo::NodeId;
use rand::{weak_rng, Rng, XorShiftRng};

/// Configuration holds the state of the membership of the cluster.
pub struct Configuration {
    peers: HashMap<NodeId, SocketAddr>,
    current: (NodeId, SocketAddr),
    rand: XorShiftRng,
}

impl Configuration {
    /// Creates a new configuration
    pub fn new<I>(current: (NodeId, SocketAddr), peers: I) -> Configuration
    where
        I: Iterator<Item = (NodeId, SocketAddr)>,
    {
        Configuration {
            peers: peers.collect(),
            current,
            rand: weak_rng(),
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

    /// Iterator containing `NodeId` values of peers
    pub fn peers(&self) -> PeerIter {
        PeerIter {
            iter: self.peers.keys(),
        }
    }

    /// Randomly selects a peer to transmit messages.
    pub fn random_peer(&mut self) -> Option<NodeId> {
        let n = self.rand.gen_range(0, self.peers.len());
        self.peers.keys().skip(n).next().cloned()
    }

    /// Gets the address of a node.
    pub fn address(&self, node: NodeId) -> Option<SocketAddr> {
        if node == self.current.0 {
            Some(self.current.1)
        } else {
            self.peers.get(&node).cloned()
        }
    }
}

/// Iterator for the peers
pub struct PeerIter<'a> {
    iter: hash_map::Keys<'a, NodeId, SocketAddr>,
}

impl<'a> Iterator for PeerIter<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().cloned()
    }
}
