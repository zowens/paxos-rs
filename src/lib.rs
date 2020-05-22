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
//! # extern crate paxos;
//! # use paxos::{Replica, Configuration};
//!
//! # fn main() {
//! let config = Configuration::new(
//!     (0u32, "127.0.0.1:4000".parse().unwrap()),
//!     vec![(1, "127.0.0.1:4001".parse().unwrap()),
//!          (2, "127.0.0.1:4002".parse().unwrap())].into_iter());
//!
//! unimplemented!("TODO: finish example");
//! # }
//! ```
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate bytes;
extern crate either;
#[macro_use]
extern crate log;
extern crate rand;
#[cfg(test)]
extern crate test;
#[cfg(test)]
extern crate lazy_static;

pub mod config;
mod multi_paxos;
mod paxos;
mod statemachine;

pub use config::Configuration;
pub use statemachine::ReplicatedState;
pub use multi_paxos::*;
