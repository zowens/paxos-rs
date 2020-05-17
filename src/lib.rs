#![feature(test)]
#![allow(unknown_lints)]
//! Rust implementation of the Paxos algorithm for replicated state machines.
//!
//! The implementation of multi-decree paxos uses multiple instances of the Paxos consus algorithm
//! to chain together commands against the replicated state machine.
//!
//! # Examples
//!
//! ```rust,no_run
//! # extern crate tokio;
//! # extern crate paxos;
//! # use paxos::{MultiPaxosBuilder, Configuration, UdpServer};
//!
//! # fn main() {
//! let config = Configuration::new(
//!     (0u32, "127.0.0.1:4000".parse().unwrap()),
//!     vec![(1, "127.0.0.1:4001".parse().unwrap()),
//!          (2, "127.0.0.1:4002".parse().unwrap())].into_iter());
//!
//! let (proposal_sink, multipaxos) = MultiPaxosBuilder::new(config.clone()).build();
//!
//! let server = UdpServer::new(config).unwrap();
//! server.run(multipaxos);
//! # }
//! ```
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate bytes;
extern crate either;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;
#[cfg(test)]
extern crate test;
extern crate tokio;
extern crate tokio_util;
extern crate pin_project;

pub mod config;
mod master;
pub mod messages;
pub mod multipaxos;
mod net;
pub mod paxos;
mod proposals;
mod state;
mod statemachine;
mod timer;
mod bytes_value;

pub use config::Configuration;
use master::{DistinguishedProposer, MasterStrategy, Masterless};
use multipaxos::MultiPaxos;
pub use net::UdpServer;
pub use proposals::ProposalSender;
pub use statemachine::ReplicatedState;

/// An instance is a _round_ of the Paxos algorithm. Instances are chained to
/// form a sequence of values. Once an instance receives consensus, the next
/// instance is started.
///
/// In some implementations, this is also called a _slot_.
pub type Instance = u64;

/// Builder for the MultiPaxos node
pub struct MultiPaxosBuilder<R: ReplicatedState, M: MasterStrategy> {
    state_machine: R,
    config: Configuration,
    master_strategy: M,
}

impl <R: ReplicatedState> MultiPaxosBuilder<R, DistinguishedProposer> {
    /// Creates a default implementation of MultiPaxos that uses a `Register` as the state machine,
    /// `DistinguishedProposer` master strategy, and default scheduler.
    pub fn new(
        config: Configuration,
        state_machine: R,
    ) -> MultiPaxosBuilder<R, DistinguishedProposer>
    {
        let master_strategy = DistinguishedProposer::new(config.clone());
        MultiPaxosBuilder {
            state_machine,
            config,
            master_strategy,
        }
    }
}

impl<R: ReplicatedState, M: MasterStrategy> MultiPaxosBuilder<R, M> {

    /// Sets the master strategy to utilize masterless
    pub fn with_masterless_strategy(self) -> MultiPaxosBuilder<R, Masterless> {
        let master_strategy = Masterless::new(self.config.clone());
        MultiPaxosBuilder {
            state_machine: self.state_machine,
            config: self.config,
            master_strategy,
        }
    }

    /// Sets the master strategy to utilize a distinguished proposer
    pub fn with_distinguished_proposer(self) -> MultiPaxosBuilder<R, DistinguishedProposer> {
        let master_strategy =
            DistinguishedProposer::new(self.config.clone());
        MultiPaxosBuilder {
            state_machine: self.state_machine,
            config: self.config,
            master_strategy,
        }
    }

    /// Builds the multi-paxos instance
    pub fn build(self) -> (ProposalSender, MultiPaxos<R, M>) {
        let (sink, stream) = proposals::proposal_channel();
        let multi_paxos = MultiPaxos::new(
            stream,
            self.state_machine,
            self.config,
            self.master_strategy,
        );
        (sink, multi_paxos)
    }
}
