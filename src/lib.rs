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
//! # use paxos::{Register, MultiPaxos, Configuration, UdpServer, proposal_channel};
//! let register = Register::default();
//! let config = Configuration::new(
//!     (0u32, "127.0.0.1:4000".parse().unwrap()),
//!     vec![(1, "127.0.0.1:4001".parse().unwrap()),
//!          (2, "127.0.0.1:4002".parse().unwrap())].into_iter());
//!
//! let (proposal_sink, proposal_stream) = proposal_channel();
//! let multipaxos = MultiPaxos::new(proposal_stream, register, config.clone());
//!
//! let server = UdpServer::new(config).unwrap();
//! server.run(multipaxos).unwrap();
//! ```
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate either;
#[macro_use]
extern crate futures;
extern crate futures_timer;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;
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
mod proposals;

pub use multipaxos::{Instance, MultiPaxos};
pub use statemachine::ReplicatedState;
pub use net::UdpServer;
pub use register::Register;
pub use config::{Configuration, PeerIntoIter, PeerIter};
pub use timer::{FuturesScheduler, Scheduler};
pub use algo::{BytesValue, NodeId, Value};
pub use proposals::{proposal_channel, ProposalReceiver, ProposalSender};
