#![allow(unknown_lints)]
//! Rust implementation of the Paxos algorithm for replicated state machines.
//!
//! The implementation of multi-decree paxos uses multiple instances of the
//! Paxos consus algorithm to chain together commands against the replicated
//! state machine.
//!
//! # Examples
//!
//! ```rust,no_run
//! # extern crate paxos;
//! # use paxos::{Replica, Configuration};
//! # use bytes::Bytes;
//!
//! # fn main() {
//! let config = Configuration::new(
//!     0u32,
//!     vec![(1, Bytes::from("127.0.0.1:4001").into()),
//!          (2, Bytes::from("127.0.0.1:4002").into())].into_iter());
//!
//! unimplemented!("TODO: finish example");
//! # }
//! ```
extern crate bytes;
#[macro_use]
extern crate log;
#[cfg(test)]
extern crate lazy_static;

mod acceptor;
mod commands;
mod config;
pub mod liveness;
mod node;
mod proposer;
pub mod statemachine;
mod window;

use std::cmp;

pub use commands::{Commander, Transport};
pub use config::{Configuration, NodeMetadata};
pub use node::Node;
pub use statemachine::ReplicatedState;
use std::marker::Sized;
pub use window::DecisionSet;

/// Increasing sequence number of Paxos instances.
pub type Slot = u64;

/// A `NodeId` is a unique value that identifies a node
/// within the configuration.
pub type NodeId = u32;

/// Ballot numbering is an increasing number in order to order proposals
/// across multiple nodes. Ballots are unique in that ballot numbers between
/// nodes are unique and it is algorithmically increasing per node.
#[derive(PartialEq, Hash, Eq, Clone, Copy, Debug)]
pub struct Ballot(pub u32, pub NodeId);

impl Ballot {
    /// Generates a ballot that is greater than `self` for a given node.
    pub fn higher_for(&self, n: NodeId) -> Ballot {
        // slight optimization to not increase ballot numeral unnecessarily
        if self.1 < n { Ballot(self.0, n) } else { Ballot(self.0 + 1, n) }
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Ballot) -> Option<cmp::Ordering> {
        match self.0.cmp(&other.0) {
            cmp::Ordering::Equal => self.1.partial_cmp(&other.1),
            o => Some(o),
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Ballot) -> cmp::Ordering {
        match self.0.cmp(&other.0) {
            cmp::Ordering::Equal => self.1.cmp(&other.1),
            o => o,
        }
    }
}

pub trait Replica: Commander {
    /// Proposes that the current node take over leadership
    fn propose_leadership(&mut self);

    /// Determines if the current node is the leader
    fn is_leader(&self) -> bool;

    /// Resolved slots within the replica
    fn decisions(&self) -> DecisionSet;

    /// Runs logic for a tick of an interval
    fn tick(&mut self);

    /// Configures the replica to re-establish leadership upon
    /// failure.
    fn liveness(self) -> liveness::Liveness<Self>
    where
        Self: Sized,
    {
        liveness::Liveness::new(self)
    }

    /// Configures the replica to use a custom state machine to apply decisions
    fn state_machine<R: ReplicatedState>(
        self,
        state_machine: R,
    ) -> statemachine::StateMachineReplica<Self, R>
    where
        Self: Sized,
    {
        statemachine::StateMachineReplica::new(self, state_machine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn ballot_higher_for() {
        let b = Ballot(6, 5);
        assert_eq!(Ballot(6, 6), b.higher_for(6));
        assert_eq!(Ballot(7, 5), b.higher_for(5));
        assert_eq!(Ballot(7, 1), b.higher_for(1));
    }
}
