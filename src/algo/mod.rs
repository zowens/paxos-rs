mod messages;
mod instance;


use std::cmp::Ordering;
use std::rc::Rc;
use std::ops::Deref;
pub use self::instance::PaxosInstance;
pub use self::messages::*;

/// Ballot numbering is an increasing number in order to order proposals
/// across multiple nodes. Ballots are unique in that ballot numbers between
/// nodes are unique and it is algorithmically increasing per node.
#[derive(PartialEq, Hash, Eq, Clone, Copy, Debug)]
pub struct Ballot(pub u32, pub NodeId);

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

/// A `NodeId` is a unique value that identifies a node
/// within the configuration.
pub type NodeId = u32;

/// Command value that the cluster members agree upon to reach consensus.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value(Rc<[u8]>);

impl From<Vec<u8>> for Value {
    fn from(vec: Vec<u8>) -> Value {
        Value(vec.into_boxed_slice().into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value(s.into_boxed_str().into_boxed_bytes().into())
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
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
struct QuorumSet {
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

    /// Flag indicating whether quorum has been reached.
    pub fn has_quorum(&self) -> bool {
        let s = &self.values;
        assert!(s.len() > 0);
        s[s.len() - 1].is_some()
    }

    #[inline]
    fn binary_search(&self, n: NodeId) -> Result<usize, usize> {
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

    /// Removes a node from the set
    pub fn remove(&mut self, n: NodeId) {
        let loc = self.binary_search(n);
        if let Ok(ind) = loc {
            self.values[ind] = None;

            // move all elements to the left to fill the hole
            let len = self.values.len();
            for i in ind + 1..len {
                self.values.swap(i - 1, i);
            }
        }
    }

    /// Flag indicating whether the set is empty
    pub fn is_empty(&self) -> bool {
        self.values[0].is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test;

    #[test]
    fn ballot_cmp() {
        let b = Ballot(5, 0);
        assert!(Ballot(2, 0).lt(&b));
        assert!(Ballot(8, 0).gt(&b));
        assert_eq!(Ballot(5, 0), b);
        assert!(b.ge(&b));
        assert!(b.le(&b));
        assert!(Ballot(5, 1).gt(&b));
    }

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

        qs.remove(5);
        assert!(!qs.contains(5));
        assert!(qs.contains(7));
        assert!(qs.contains(2));
        assert!(qs.contains(6));
        assert!(!qs.has_quorum());
        assert!(!qs.is_empty());
        assert_eq!(&[Some(2), Some(6), Some(7), None], qs.values.as_ref());

        qs.insert(1);
        qs.remove(7);
        assert_eq!(&[Some(1), Some(2), Some(6), None], qs.values.as_ref());

        // ignroe adds when there is quorum
        qs.insert(7);
        assert_eq!(&[Some(1), Some(2), Some(6), Some(7)], qs.values.as_ref());
        qs.insert(10);
        assert_eq!(&[Some(1), Some(2), Some(6), Some(7)], qs.values.as_ref());
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
            qs.insert(8);
            qs.insert(8);
            qs.remove(5);
            qs.is_empty();
            qs.insert(10);
            qs.insert(3);
        })
    }
}
