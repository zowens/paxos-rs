#![feature(conservative_impl_trait, option_filter)]
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate log;

mod messenger;
mod instance;

use std::cmp::Ordering;
use std::collections::HashSet;

/// Ballot numbering is an increasing number in order to order proposals
/// across multiple nodes. Ballots are unique in that ballot numbers between
/// nodes are unique and it is algorithmically increasing per node.
#[derive(PartialEq, Hash, Eq, Clone, Copy, Debug)]
pub struct Ballot(u64, NodeId);

impl Ballot {
    /// Generates a ballot that is greater than `self` for a given node.
    pub fn higher_for(&self, n: NodeId) -> Ballot {
        // slight optimization to not increase ballot numeral unnecessarily
        if self.1 < n {
            Ballot(self.0, n)
        } else {
            Ballot(self.0 + 1, n)
        }
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Ballot) -> Option<Ordering> {
        match self.0.cmp(&other.0) {
            Ordering::Equal => self.1.partial_cmp(&other.1),
            o => Some(o),
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Ballot) -> Ordering {
        match self.0.cmp(&other.0) {
            Ordering::Equal => self.1.cmp(&other.1),
            o => o,
        }
    }
}

/// An instance is a "round" of Paxos. Instances are chained to
/// form a sequence of values.
pub type Instance = u64;

/// A `NodeId` is a unique value that identifies a node
/// within the configuration.
pub type NodeId = u64;

// TODO: parameterize Value within PaxosInstance
pub type Value = Vec<u8>;

/// `QuorumSet` tracks nodes that have sent certain messages and will
/// detect when quorum is reached. Duplicates are treated as a single
/// message to determine quorum.
#[derive(Clone, Debug)]
struct QuorumSet {
    quorum_size: usize,
    values: HashSet<NodeId>,
}

impl QuorumSet {
    /// Creates a QuorumSet with a given size for quorum.
    pub fn with_size(size: usize) -> QuorumSet {
        QuorumSet {
            quorum_size: size,
            values: HashSet::with_capacity(size),
        }
    }

    /// Flag indicating whether quorum has been reached.
    pub fn has_quorum(&self) -> bool {
        self.values.len() >= self.quorum_size
    }

    /// Inserts a node into the set
    pub fn insert(&mut self, n: NodeId) {
        self.values.insert(n);
    }

    /// Flag indicating whether the set contains a given node
    pub fn contains(&self, n: NodeId) -> bool {
        self.values.contains(&n)
    }

    /// Removes a node from the set
    pub fn remove(&mut self, n: NodeId) {
        self.values.remove(&n);
    }

    /// Flag indicating whether the set is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}


pub struct Configuration {}

impl Configuration {
    fn quorum_size(&self) -> usize {
        // TODO: ...
        2
    }

    fn current(&self) -> NodeId {
        unimplemented!("")
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ballot_cmp() {
        let b = Ballot(5, 0);
        assert!(Ballot(2, 0).lt(&b));
        assert!(Ballot(8, 0).gt(&b));
        assert_eq!(Ballot(5, 0), b);
        assert!(b.ge(&b));
        assert!(b.le(&b));
        assert!(Ballot(5, 1).gt(&b));
    }

    #[test]
    fn test_quorumset() {
        let mut qs = QuorumSet::with_size(3);

        assert!(!qs.has_quorum());
        assert!(qs.is_empty());

        qs.insert(5);
        assert!(qs.contains(5));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());

        qs.insert(6);
        assert!(qs.contains(6));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());

        qs.insert(6);
        assert!(qs.contains(6));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());

        qs.insert(7);
        assert!(qs.contains(7));
        assert!(qs.has_quorum());
        assert!(!qs.is_empty());

        qs.remove(5);
        assert!(!qs.contains(5));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
    }
}
