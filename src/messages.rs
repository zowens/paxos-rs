use std::io;
use std::net::SocketAddr;

use super::Instance;
use algo::{Accept, Accepted, Ballot, NodeId, Prepare, Promise, Reject, Value};

use capnp::{Error as CapnpError, NotInSchema};
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize_packed::{read_message, write_message};
use messages_capnp;

#[derive(Clone, Debug)]
pub enum MultiPaxosMessage {
    Prepare(Instance, Prepare),
    Promise(Instance, Promise),
    Accept(Instance, Accept),
    Accepted(Instance, Accepted),
    Reject(Instance, Reject),
    Sync(NodeId, Instance),
    Catchup(NodeId, Instance, Value),
}

#[derive(Debug)]
pub enum DeserializeError {
    Capnp(CapnpError),
    CapnpNotInSchema(NotInSchema),
}

impl From<CapnpError> for DeserializeError {
    fn from(e: CapnpError) -> DeserializeError {
        DeserializeError::Capnp(e)
    }
}

impl From<NotInSchema> for DeserializeError {
    fn from(e: NotInSchema) -> DeserializeError {
        DeserializeError::CapnpNotInSchema(e)
    }
}

impl Into<io::Error> for DeserializeError {
    fn into(self) -> io::Error {
        error!("Error reading Paxos message from peer: {:?}", self);
        io::Error::new(io::ErrorKind::InvalidData, "Invalid Paxos Message")
    }
}

#[inline]
fn serialize_ballot(proposal: Ballot, mut builder: messages_capnp::ballot::Builder) {
    builder.set_id(proposal.0);
    builder.set_node_id(proposal.1);
}

#[inline]
fn deserialize_ballot(reader: messages_capnp::ballot::Reader) -> Ballot {
    Ballot(reader.get_id(), reader.get_node_id())
}

impl MultiPaxosMessage {
    pub fn deserialize(peer: NodeId, buf: &[u8]) -> Result<MultiPaxosMessage, DeserializeError> {
        let mut cursor = io::Cursor::new(buf);
        let reader = read_message(&mut cursor, ReaderOptions::new())?;

        use messages_capnp::paxos_message::Which as WhichMsg;
        use messages_capnp::paxos_message::promise::Which as WhichLastAccepted;
        let paxos_msg = reader.get_root::<messages_capnp::paxos_message::Reader>()?;
        let inst = { paxos_msg.borrow().get_instance() };
        match paxos_msg.which()? {
            WhichMsg::Prepare(prepare) => {
                let prepare = prepare?;
                let proposal = deserialize_ballot(prepare.get_proposal()?);
                Ok(MultiPaxosMessage::Prepare(inst, Prepare(peer, proposal)))
            }
            WhichMsg::Promise(promise) => {
                let promise = promise?;
                let proposal = { deserialize_ballot(promise.borrow().get_proposal()?) };

                let last_accepted = {
                    match promise.which()? {
                        WhichLastAccepted::NoneAccepted(_) => None,
                        WhichLastAccepted::LastAccepted(last_accepted) => {
                            let last_accepted = last_accepted?;
                            let bal =
                                { deserialize_ballot(last_accepted.borrow().get_proposal()?) };
                            let val = last_accepted.get_value()?;
                            Some((bal, Vec::from(val)))
                        }
                    }
                };

                Ok(MultiPaxosMessage::Promise(
                    inst,
                    Promise(peer, proposal, last_accepted),
                ))
            }
            WhichMsg::Accept(accept) => {
                let accept = accept?;
                let proposal = { deserialize_ballot(accept.borrow().get_proposal()?) };
                let value = accept.get_value()?;
                Ok(MultiPaxosMessage::Accept(
                    inst,
                    Accept(peer, proposal, value.to_vec()),
                ))
            }
            WhichMsg::Accepted(accepted) => {
                let accepted = accepted?;
                let proposal = { deserialize_ballot(accepted.borrow().get_proposal()?) };
                let value = accepted.get_value()?;
                Ok(MultiPaxosMessage::Accepted(
                    inst,
                    Accepted(peer, proposal, value.to_vec()),
                ))
            }
            WhichMsg::Reject(reject) => {
                let reject = reject?;
                let proposal = { deserialize_ballot(reject.borrow().get_proposal()?) };
                let promised = { deserialize_ballot(reject.borrow().get_promised()?) };
                Ok(MultiPaxosMessage::Reject(
                    inst,
                    Reject(peer, proposal, promised),
                ))
            }
            WhichMsg::Sync(sync_req) => {
                let _ = sync_req?;
                Ok(MultiPaxosMessage::Sync(peer, inst))
            }
            WhichMsg::Catchup(catchup) => {
                let catchup = catchup?;
                let value = catchup.get_value()?;
                Ok(MultiPaxosMessage::Catchup(peer, inst, value.to_vec()))
            }
        }
    }

    pub fn peer(&self) -> NodeId {
        match *self {
            MultiPaxosMessage::Prepare(_, Prepare(peer, _))
            | MultiPaxosMessage::Promise(_, Promise(peer, _, _))
            | MultiPaxosMessage::Accept(_, Accept(peer, _, _))
            | MultiPaxosMessage::Accepted(_, Accepted(peer, _, _))
            | MultiPaxosMessage::Reject(_, Reject(peer, _, _))
            | MultiPaxosMessage::Sync(peer, _)
            | MultiPaxosMessage::Catchup(peer, _, _) => peer,
        }
    }

    pub fn serialize<W>(self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        let mut builder = Builder::new(HeapAllocator::new());

        match self {
            MultiPaxosMessage::Prepare(inst, Prepare(_peer, proposal)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let prepare = msg.borrow().init_prepare();
                let ballot = prepare.init_proposal();
                serialize_ballot(proposal, ballot);
            }
            MultiPaxosMessage::Promise(inst, Promise(_peer, proposal, last_accepted)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut promise = msg.borrow().init_promise();
                {
                    let ballot = promise.borrow().init_proposal();
                    serialize_ballot(proposal, ballot);
                }

                match last_accepted {
                    Some((bal, val)) => {
                        let mut last_accepted = promise.borrow().init_last_accepted();
                        last_accepted.set_value(&val);
                        let last_accepted_proposal = last_accepted.init_proposal();
                        serialize_ballot(bal, last_accepted_proposal);
                    }
                    None => {
                        promise.set_none_accepted(());
                    }
                }
            }
            MultiPaxosMessage::Accept(inst, Accept(_peer, proposal, value)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut accept = msg.borrow().init_accept();

                {
                    let ballot = accept.borrow().init_proposal();
                    serialize_ballot(proposal, ballot);
                }

                // TODO: how do we move instead of copy
                accept.set_value(&value);
            }
            MultiPaxosMessage::Accepted(inst, Accepted(_peer, proposal, value)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut accepted = msg.borrow().init_accepted();
                {
                    let ballot = accepted.borrow().init_proposal();
                    serialize_ballot(proposal, ballot);
                }
                accepted.set_value(&value);
            }
            MultiPaxosMessage::Reject(inst, Reject(_peer, proposal, promised)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut reject = msg.borrow().init_reject();
                {
                    let ballot = reject.borrow().init_proposal();
                    serialize_ballot(proposal, ballot);
                }
                {
                    let ballot = reject.borrow().init_promised();
                    serialize_ballot(promised, ballot);
                }
            }
            MultiPaxosMessage::Sync(_peer, inst) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                msg.borrow().init_sync();
            }
            MultiPaxosMessage::Catchup(_peer, inst, value) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut catchup = msg.borrow().init_catchup();
                catchup.set_value(&value);
            }
        }

        write_message(write, &builder)
    }
}

#[derive(Clone, Debug)]
pub enum ClientMessage {
    /// Proposes new value
    ProposeRequest(SocketAddr, Value),

    /// Requests that the current value be sent back
    LookupValueRequest(SocketAddr),

    /// Current value reply
    CurrentValueResponse(SocketAddr, Value),

    /// No current value
    NoValueResponse(SocketAddr),
}

impl ClientMessage {
    pub fn deserialize(addr: SocketAddr, buf: &[u8]) -> Result<ClientMessage, DeserializeError> {
        let mut cursor = io::Cursor::new(buf);
        let reader = read_message(&mut cursor, ReaderOptions::new())?;

        use messages_capnp::client_message::Which as WhichMsg;

        let client_msg = reader.get_root::<messages_capnp::client_message::Reader>()?;
        match client_msg.which()? {
            WhichMsg::ProposeValueRequest(propose) => {
                let value = propose?.get_value()?.to_vec();
                Ok(ClientMessage::ProposeRequest(addr, value))
            }
            WhichMsg::LookupValueRequest(lookup) => {
                let _ = lookup?;
                Ok(ClientMessage::LookupValueRequest(addr))
            }
            WhichMsg::CurrentValueResponse(current_val) => {
                let value = current_val?.get_value()?.to_vec();
                Ok(ClientMessage::CurrentValueResponse(addr, value))
            }
            WhichMsg::NoCurrentValueResponse(no_current_val) => {
                let _ = no_current_val?;
                Ok(ClientMessage::NoValueResponse(addr))
            }
        }
    }

    pub fn serialize<W>(self, write: &mut W) -> io::Result<SocketAddr>
    where
        W: io::Write,
    {
        let mut builder = Builder::new(HeapAllocator::new());

        let addr = match self {
            ClientMessage::ProposeRequest(addr, value) => {
                let mut msg = builder.init_root::<messages_capnp::client_message::Builder>();
                let mut propose = msg.init_propose_value_request();
                propose.set_value(&value);
                addr
            }
            ClientMessage::LookupValueRequest(addr) => {
                let mut msg = builder.init_root::<messages_capnp::client_message::Builder>();
                msg.init_lookup_value_request();
                addr
            }
            ClientMessage::CurrentValueResponse(addr, value) => {
                let mut msg = builder.init_root::<messages_capnp::client_message::Builder>();
                let mut current_val = msg.init_current_value_response();
                current_val.set_value(&value);
                addr
            }
            ClientMessage::NoValueResponse(addr) => {
                let mut msg = builder.init_root::<messages_capnp::client_message::Builder>();
                msg.init_no_current_value_response();
                addr
            }
        };

        write_message(write, &builder)?;
        Ok(addr)
    }
}

/// Message handled by `MultiPaxos`
pub enum Message {
    /// Message sent within the cluster
    MultiPaxos(MultiPaxosMessage),
    /// Message sent from an outside client
    Client(ClientMessage),
}
