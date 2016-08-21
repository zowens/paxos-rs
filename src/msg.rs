use std::io;

use capnp;
use capnp::message::{HeapAllocator, Builder as MessageBuilder};

use super::messages_capnp;

pub type Acceptor = String;
pub type Value = Vec<u8>;
pub type Ballot = u64;
pub enum PaxosMessage {
    Prepare(Ballot),
    Promise(Acceptor, Ballot, Option<(Ballot, Value)>),
    Accept(Ballot, Value),
    Accepted(Acceptor, Ballot, Value),
}

#[inline]
fn set<'a, B, F>(b: &'a mut MessageBuilder<HeapAllocator>, f: F)
    where B: capnp::traits::FromPointerBuilder<'a>,
          F: FnOnce(B) -> ()
{
    let r = b.init_root::<B>();
    f(r);
}

#[inline]
fn reader_from<'a, R>(b: &'a MessageBuilder<HeapAllocator>) -> R
    where R: capnp::traits::FromPointerReader<'a>
{
    b.get_root_as_reader::<R>().unwrap()
}

macro_rules! data {
    ($e: ident) => (
        capnp::data::new_reader($e.as_slice().as_ptr(), $e.len() as u32);
    )
}

impl PaxosMessage {
    pub fn write_to<W>(self, w: &mut W) -> io::Result<()>
        where W: io::Write
    {
        use ::messages_capnp::paxos_message::*;

        let mut outer_builder = MessageBuilder::new_default();
        set(&mut outer_builder, move |mut r: Builder| {
            let mut inner_builder = MessageBuilder::new_default();
            match self {
                PaxosMessage::Prepare(ballot) => {
                    set(&mut inner_builder,
                        |mut p: prepare::Builder| p.set_ballot(ballot));
                    r.set_prepare(reader_from(&inner_builder)).unwrap();
                }
                PaxosMessage::Promise(acceptor, ballot, last_value) => {
                    set(&mut inner_builder, move |mut promise: promise::Builder| {
                        let mut acceptor_builder = MessageBuilder::new_default();
                        set(&mut acceptor_builder,
                            |mut a: messages_capnp::acceptor::Builder| a.set_address(&acceptor));
                        promise.set_acceptor(reader_from(&acceptor_builder)).unwrap();

                        promise.set_ballot(ballot);

                        match last_value {
                            Some((last_ballot, last_val)) => {
                                let mut last_accepted_builder = MessageBuilder::new_default();
                                set(&mut last_accepted_builder,
                                    |mut la: promise::last_accepted::Builder| {
                                        la.set_ballot(last_ballot);
                                        la.set_value(data!(last_val));
                                    });
                                promise.set_last_accepted(reader_from(&last_accepted_builder))
                                    .unwrap();
                            }
                            None => promise.set_none_accepted(()),
                        }
                    });
                    r.set_promise(reader_from(&inner_builder)).unwrap();
                }
                PaxosMessage::Accept(ballot, value) => {
                    set(&mut inner_builder, |mut a: accept::Builder| {
                        a.set_ballot(ballot);
                        a.set_value(data!(value));
                    });
                    r.set_accept(reader_from(&inner_builder)).unwrap();
                }
                PaxosMessage::Accepted(acceptor, ballot, value) => {
                    set(&mut inner_builder, |mut a: accepted::Builder| {
                        let mut acceptor_builder = MessageBuilder::new_default();
                        set(&mut acceptor_builder,
                            |mut a: messages_capnp::acceptor::Builder| a.set_address(&acceptor));
                        a.set_acceptor(reader_from(&acceptor_builder)).unwrap();
                        a.set_ballot(ballot);
                        a.set_value(data!(value));
                    });
                    r.set_accepted(reader_from(&inner_builder)).unwrap();
                }
            }
        });

        capnp::serialize_packed::write_message(w, &outer_builder)
    }
}
