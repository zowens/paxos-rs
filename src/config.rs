use std::collections::HashMap;
use std::collections::hash_map;
use std::net::SocketAddr;
use std::rc::Rc;
use std::cell::{Ref, RefCell};
use algo::NodeId;
use rand::{weak_rng, Rng, XorShiftRng};

struct Inner {
    peers: HashMap<NodeId, SocketAddr>,
    socket_to_peer: HashMap<SocketAddr, NodeId>,
    rand: XorShiftRng,
}

/// Configuration holds the state of the membership of the cluster.
#[derive(Clone)]
pub struct Configuration {
    current: (NodeId, SocketAddr),
    inner: Rc<RefCell<Inner>>,
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
            inner: Rc::new(RefCell::new(Inner {
                peers,
                socket_to_peer,
                rand: weak_rng(),
            })),
        }
    }

    /// Size of quorum
    pub fn quorum_size(&self) -> usize {
        1 + (self.inner.borrow().peers.len() / 2)
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
        PeerIntoIter {
            r: self.inner.borrow(),
        }
    }

    /// Sets the membership of peers. This allows for reconfiguration.
    pub fn set_peers<I>(&self, peers: I)
    where
        I: Iterator<Item = (NodeId, SocketAddr)>,
    {
        let peers: HashMap<NodeId, SocketAddr> = peers.collect();
        let socket_to_peer: HashMap<SocketAddr, NodeId> =
            peers.iter().map(|e| (*e.1, *e.0)).collect();

        let mut v = self.inner.borrow_mut();
        v.peers = peers;
        v.socket_to_peer = socket_to_peer;
    }

    /// Randomly selects a peer to transmit messages.
    pub fn random_peer(&mut self) -> Option<NodeId> {
        let len = self.inner.borrow().peers.len();
        let n = self.inner.borrow_mut().rand.gen_range(0, len);
        self.inner.borrow().peers.keys().skip(n).next().cloned()
    }

    /// Gets the address of a node.
    pub fn address(&self, node: NodeId) -> Option<SocketAddr> {
        if node == self.current.0 {
            Some(self.current.1)
        } else {
            self.inner.borrow().peers.get(&node).cloned()
        }
    }

    /// Gets the peer ID from a socket address.
    pub fn peer_id(&self, address: &SocketAddr) -> Option<NodeId> {
        if address == &self.current.1 {
            Some(self.current.0)
        } else {
            self.inner.borrow().socket_to_peer.get(address).cloned()
        }
    }
}

/// `IntoIterator` for peers
pub struct PeerIntoIter<'a> {
    r: Ref<'a, Inner>,
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
