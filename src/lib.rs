extern crate capnp;

mod msg;

#[allow(dead_code)]
mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

// re-export msg values
pub use msg::*;
