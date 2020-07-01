use crate::NodeId;
use bytes::Bytes;
use std::{collections::HashMap, fmt};

#[derive(Default, Clone, Debug, Eq, PartialEq)]
/// Opaque, applicaiton specific metadata for nodes in the system
pub struct NodeMetadata(pub Bytes);

impl From<Bytes> for NodeMetadata {
    fn from(v: Bytes) -> NodeMetadata {
        NodeMetadata(v)
    }
}

impl Into<Bytes> for NodeMetadata {
    fn into(self) -> Bytes {
        self.0
    }
}

/// Configuration holds the state of the membership of the cluster.
#[derive(Clone)]
pub struct Configuration {
    current: NodeId,
    peers: HashMap<NodeId, NodeMetadata>,
}

impl Configuration {
    /// Creates a new configuration
    pub fn new<I>(current: NodeId, peers: I) -> Configuration
    where
        I: Iterator<Item = (NodeId, NodeMetadata)>,
    {
        let peers: HashMap<NodeId, NodeMetadata> = peers.collect();
        Configuration { current, peers }
    }

    /// Size of phase 1 and phase 2 quorums.
    pub fn quorum_size(&self) -> (usize, usize) {
        // TODO: allow flexible quorum
        let size = 1 + (self.peers.len() / 2);
        (size, size)
    }

    /// Current node identifier
    pub fn current(&self) -> NodeId {
        self.current
    }

    /// Iterator containing `NodeId` values of peers
    pub fn peer_node_ids<'a>(&'a self) -> impl Iterator<Item = NodeId> + 'a {
        self.peers.keys().copied()
    }

    /// Iterator containing all nodes along with their metadata
    pub fn peers<'a>(&'a self) -> impl Iterator<Item = (NodeId, &NodeMetadata)> + 'a {
        self.peers.iter().map(|(id, meta)| (*id, meta))
    }
}

impl fmt::Debug for Configuration {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (p1_q, p2_q) = self.quorum_size();
        fmt.debug_struct("Configuration")
            .field("current_node_id", &self.current)
            .field("peers", &self.peers)
            .field("phase_1_quorum", &p1_q)
            .field("phase_2_quorum", &p2_q)
            .finish()
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
        QuorumSet { values: vec![None; size].into_boxed_slice() }
    }

    /// Size of the quorum
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Flag indicating whether quorum has been reached.
    pub fn has_quorum(&self) -> bool {
        self.values.last().unwrap().is_some()
    }

    #[inline]
    fn search(&self, n: NodeId) -> Result<usize, usize> {
        self.values.iter().enumerate().find_map(|(i, elem)| {
            match elem {
                Some(nd) if *nd == n => Some(Ok(i)),
                None => Some(Err(i)),
                _ => None,
            }
        }).unwrap_or_else(|| Err(self.values.len()-1))
    }

    /// Inserts a node into the set
    pub fn insert(&mut self, n: NodeId) {
        if let Err(loc) = self.search(n) {
            self.values[loc] = Some(n);
        }
    }

    /// Flag indicating whether the set contains a given node
    pub fn contains(&self, n: NodeId) -> bool {
        self.search(n).is_ok()
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
        assert_eq!(&[Some(5), Some(7), Some(2), None], qs.values.as_ref());

        qs.insert(6);
        assert!(qs.contains(5));
        assert!(qs.contains(7));
        assert!(qs.contains(2));
        assert!(qs.contains(6));
        assert!(qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(5), Some(7), Some(2), Some(6)], qs.values.as_ref());

        qs.insert(10);
        assert_eq!(&[Some(5), Some(7), Some(2), Some(10)], qs.values.as_ref());
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
