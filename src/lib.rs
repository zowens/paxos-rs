#![feature(option_filter)]
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate log;
extern crate rand;

mod algo;
mod messenger;
mod multipaxos;
mod register;
mod retry;
mod config;

/// An instance is a "round" of Paxos. Instances are chained to
/// form a sequence of values.
///
/// In some implementations, this is also called a "Slot"
pub type Instance = u64;
