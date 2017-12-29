#![feature(test, option_filter)]
#![allow(unknown_lints)]
//! Rust implementation of the Paxos algorithm for replicated state machines.
//!
//! The implementation of multi-decree paxos uses multiple instances of the Paxos consus algorithm
//! to chain together commands against the replicated state machine.
//!
//! # Examples
//!
//! ```rust,no_run
//! # use paxos::{Register, MultiPaxos, Configuration, UdpServer};
//! let register = Register::default();
//! let config = Configuration::new(
//!     (0u32, "127.0.0.1:4000".parse().unwrap()),
//!     vec![(1, "127.0.0.1:4001".parse().unwrap()),
//!          (2, "127.0.0.1:4002".parse().unwrap())].into_iter());
//!
//! let multipaxos = MultiPaxos::new(register, config.clone()).into_networked();
//!
//! let server = UdpServer::new(&config).unwrap();
//! server.run(multipaxos).unwrap();
//! ```
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate capnp;
extern crate either;
#[macro_use]
extern crate futures;
extern crate futures_timer;
#[macro_use]
extern crate log;
extern crate rand;
#[cfg(test)]
extern crate test;
extern crate tokio_core;

mod algo;
mod state;
mod statemachine;
pub mod messages;
mod multipaxos;
mod net;
mod register;
mod config;
mod timer;

pub use multipaxos::{MultiPaxos, NetworkedMultiPaxos, ProposalSender};
pub use statemachine::ReplicatedState;
pub use net::UdpServer;
pub use register::Register;
pub use config::{Configuration, PeerIntoIter, PeerIter};
pub use timer::{FuturesScheduler, Scheduler};
pub use algo::{NodeId, Value};

/// An instance is a _round_ of the Paxos algorithm. Instances are chained to
/// form a sequence of values. Once an instance receives consensus, the next
/// instance is started.
///
/// In some implementations, this is also called a _slot_.
pub type Instance = u64;

#[allow(dead_code, clippy)]
mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}
