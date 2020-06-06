use crate::NodeId;
use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::cmp::Ordering;


/// Configuration holds the state of the membership of the cluster.
#[derive(Clone)]
pub struct Configuration {
    current: (NodeId, SocketAddr),
    peers: HashMap<NodeId, SocketAddr>,
    socket_to_peer: HashMap<SocketAddr, NodeId>,
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
        }
    }

    /// Size of phase 1 and phase 2 quorums.
    pub fn quorum_size(&self) -> (usize, usize) {
        // TODO: allow flexible quorum
        let size = 1 + (self.peers.len() / 2);
        (size, size)
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

/// `QuorumSet` tracks nodes that have sent certain messages and will
/// detect when quorum is reached. Duplicates are treated as a single
/// message to determine quorum.
///
/// Once the `QuorumSet` has quorum, additional nodes will not be added.
/// The purpose of the datastructure is to track _when_ quorum is
/// reached rather than being a general purpose set.
#[derive(Clone, Debug)]
pub struct QuorumSet {
    // Instead of using a HashSet or Vec, which may allocate more
    // than once, the QuorumSet has a specific size as a sized slice.
    // The datastructure ensures that the node IDs are stored in
    // sorted order.
    //
    // Quorums are typically small (2-4 nodes) so a smaller
    // data structure that isn't fancy is appropriate both
    // from a run time perspective and space perspective.
    values: Box<[Option<NodeId>]>,
}

impl QuorumSet {
    /// Creates a QuorumSet with a given size for quorum.
    pub fn with_size(size: usize) -> QuorumSet {
        assert!(size > 0);
        QuorumSet {
            values: vec![None; size].into_boxed_slice(),
        }
    }

    /// Size of the quorum
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Flag indicating whether quorum has been reached.
    pub fn has_quorum(&self) -> bool {
        let s = &self.values;
        assert!(s.len() > 0);
        s[s.len() - 1].is_some()
    }

    #[inline]
    fn binary_search(&self, n: NodeId) -> Result<usize, usize> {
        // TODO: remove binary search in favor of linear
        self.values.binary_search_by(move |v| match *v {
            Some(v) => v.cmp(&n),
            None => Ordering::Greater,
        })
    }

    /// Inserts a node into the set
    pub fn insert(&mut self, n: NodeId) {
        if self.has_quorum() {
            return;
        }

        let loc = self.binary_search(n);
        if let Err(loc) = loc {
            // if theres an existing occupant, then move
            // all the values over to the right to make
            // a hole for the new value in the correct
            // place
            if self.values[loc].is_some() {
                let len = self.values.len();
                for i in (loc..len - 1).rev() {
                    self.values.swap(i, i + 1);
                }
            }

            self.values[loc] = Some(n);
        }
    }

    /// Flag indicating whether the set contains a given node
    pub fn contains(&self, n: NodeId) -> bool {
        self.binary_search(n).is_ok()
    }

    /// Flag indicating whether the set is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.values[0].is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test;

    #[test]
    fn quorumset() {
        let mut qs = QuorumSet::with_size(4);

        assert!(!qs.has_quorum());
        assert!(qs.is_empty());

        qs.insert(5);
        assert!(qs.contains(5));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(5), None, None, None], qs.values.as_ref());

        qs.insert(7);
        assert!(qs.contains(7));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(5), Some(7), None, None], qs.values.as_ref());

        qs.insert(7);
        assert!(qs.contains(5));
        assert!(qs.contains(7));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(5), Some(7), None, None], qs.values.as_ref());

        qs.insert(2);
        assert!(qs.contains(5));
        assert!(qs.contains(7));
        assert!(qs.contains(2));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(2), Some(5), Some(7), None], qs.values.as_ref());

        qs.insert(6);
        assert!(qs.contains(5));
        assert!(qs.contains(7));
        assert!(qs.contains(2));
        assert!(qs.contains(6));
        assert!(qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(2), Some(5), Some(6), Some(7)], qs.values.as_ref());

        // ignroe adds when there is quorum
        qs.insert(10);
        assert_eq!(&[Some(2), Some(5), Some(6), Some(7)], qs.values.as_ref());
    }

    #[test]
    fn quorum_one() {
        let mut qs = QuorumSet::with_size(1);
        assert!(qs.is_empty());
        assert!(!qs.has_quorum());

        qs.insert(5);
        assert!(!qs.is_empty());
        assert!(qs.has_quorum());
    }

    #[bench]
    fn bench_quorum_set(b: &mut test::Bencher) {
        b.iter(|| {
            let mut qs = QuorumSet::with_size(5);
            qs.insert(5);
            qs.has_quorum();
            qs.insert(2);
            qs.has_quorum();
            qs.insert(8);
            qs.has_quorum();
            qs.insert(8);
            qs.has_quorum();
        })
    }
}
