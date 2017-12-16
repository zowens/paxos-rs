#![feature(option_filter)]
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate log;

mod algo;
mod messenger;
mod chain;

use std::collections::HashMap;
use std::collections::hash_map;
use std::net::SocketAddr;
use algo::NodeId;

/// An instance is a "round" of Paxos. Instances are chained to
/// form a sequence of values.
pub type Instance = u64;

// TODO: support reconfiguration
pub struct Configuration {
    peers: HashMap<NodeId, SocketAddr>,
    current: (NodeId, SocketAddr),
}

impl Configuration {
    pub fn new<I>(current: (NodeId, SocketAddr), peers: I) -> Configuration
    where
        I: Iterator<Item = (NodeId, SocketAddr)>,
    {
        Configuration {
            peers: peers.collect(),
            current,
        }
    }

    pub fn quorum_size(&self) -> usize {
        1 + (self.peers.len() / 2)
    }

    pub fn current(&self) -> NodeId {
        self.current.0
    }

    pub fn peers(&self) -> PeerIter {
        PeerIter {
            iter: self.peers.keys(),
        }
    }

    pub fn address(&self, node: NodeId) -> Option<SocketAddr> {
        if node == self.current.0 {
            Some(self.current.1)
        } else {
            self.peers.get(&node).cloned()
        }
    }
}

pub struct PeerIter<'a> {
    iter: hash_map::Keys<'a, NodeId, SocketAddr>,
}

impl<'a> Iterator for PeerIter<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().cloned()
    }
}
