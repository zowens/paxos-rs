#![feature(option_filter)]
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
extern crate capnp;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate rand;
extern crate tokio_core;

mod algo;
mod state;
pub mod messages;
pub mod multipaxos;
pub mod net;
pub mod register;
pub mod config;

/// An instance is a "round" of Paxos. Instances are chained to
/// form a sequence of values.
///
/// In some implementations, this is also called a "Slot"
pub type Instance = u64;

#[allow(dead_code)]
mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}
