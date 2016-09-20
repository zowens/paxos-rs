#![allow(dead_code)]
extern crate core;
#[macro_use]
extern crate matches;

mod acceptor;

use std::collections::{HashSet, HashMap};
use std::collections::hash_map::Entry;
use core::cmp::Ordering;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Ballot(u64, Node);

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

#[derive(PartialEq, PartialOrd, Ord, Eq, Hash, Copy, Clone, Debug)]
pub struct Slot(u64);

impl From<u64> for Slot {
    fn from(s: u64) -> Slot {
        Slot(s)
    }
}

#[derive(PartialEq, PartialOrd, Ord, Eq, Hash, Clone, Debug)]
pub struct Node(String);

#[derive(Clone)]
pub struct CommandSet<C: Clone> {
    s: HashMap<Slot, (Ballot, C)>,
}

impl<C> CommandSet<C>
    where C: Clone
{
    pub fn new() -> CommandSet<C> {
        CommandSet { s: HashMap::new() }
    }

    pub fn get(&self, s: Slot) -> Option<(Ballot, &C)> {
        self.s.get(&s).as_ref().map(|t| (t.0.clone(), &t.1))
    }

    pub fn add(&mut self, s: Slot, b: Ballot, c: C) {
        match self.s.entry(s) {
            Entry::Occupied(mut v) => {
                // replace the slot if the ballot stored is
                // less than the current ballot
                if v.get().0.lt(&b) {
                    v.insert((b, c));
                }
            }
            Entry::Vacant(e) => {
                // blind insert, new entry
                e.insert((b, c));
            }
        }

    }
}

pub struct Config {
    // TODO: [Co-locate]
    id: Node,
    leaders: HashSet<Node>,
    acceptors: HashSet<Node>,
    replicas: HashSet<Node>,
}

// messenger of command C
pub trait Messenger<C: Clone> {
    type Response;

    fn send(&self, n: Node, m: PaxosMessage<C>) -> Self::Response;
}

pub enum PaxosMessage<C: Clone> {
    Prepare(Ballot),
    Promise(Ballot, CommandSet<C>),
    PromiseNack(Ballot),
    Accepted(Ballot, Slot),
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ballot_cmp() {
        let b = Ballot(5, Node("a".to_string()));
        assert!(Ballot(2, Node("a".to_string())).lt(&b));
        assert!(Ballot(8, Node("a".to_string())).gt(&b));
        assert!(b.ge(&b));
        assert!(b.le(&b));
        assert!(Ballot(5, Node("b".to_string())).gt(&b));
    }

    #[test]
    fn test_commandset_add() {
        let mut cs = CommandSet::<u8>::new();

        // initially empty
        assert_eq!(None, cs.get(Slot(5)));

        {
            // then value 4
            cs.add(Slot(5), Ballot(0, Node("".to_string())), 4u8);
            let (b, v) = cs.get(Slot(5)).unwrap();
            assert_eq!(Ballot(0, Node("".to_string())), b);
            assert_eq!(4u8, *v);
        }

        {
            // now value 0
            cs.add(Slot(5), Ballot(2, Node("".to_string())), 0u8);
            let (b, v) = cs.get(Slot(5)).unwrap();
            assert_eq!(Ballot(2, Node("".to_string())), b);
            assert_eq!(0u8, *v);
        }

        {
            // stays at value 0 (ballot 1 < ballot 2)
            cs.add(Slot(5), Ballot(0, Node("".to_string())), 1u8);
            let (b, v) = cs.get(Slot(5)).unwrap();
            assert_eq!(Ballot(2, Node("".to_string())), b);
            assert_eq!(0u8, *v);
        }
    }
}
