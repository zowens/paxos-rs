use std::io;
use std::net::SocketAddr;

use super::Instance;
pub use algo::{Accept, Accepted, Ballot, NodeId, Prepare, Promise, Reject};
use algo::Value;

use capnp::{Error as CapnpError, NotInSchema};
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize_packed::{read_message, write_message};
use messages_capnp;

#[derive(Clone, Debug)]
/// Message generated within the cluster.
pub enum MultiPaxosMessage {
    /// `PREPARE` message is the Phase 1a message from a proposer sent
    /// to acceptors to receive agreement to not accept ballots of lower value.
    Prepare(Instance, Prepare),

    /// `PROMISE` is the Phase 1b message sent from acceptors in reply to
    /// `PREPARE` messages. The ballot in the promise denotes that the acceptor
    /// will not accept ballots less than the promised ballot.
    Promise(Instance, Promise),

    /// `ACCEPT` message is the Phase 2a message from a proposer sent
    /// to acceptors to accept a value. The `ACCEPT` message is predicated
    /// on the proposer receiving quorum from Phase 1.
    Accept(Instance, Accept),

    /// `ACCEPTED` is the Phase 2b message that is broadcast from acceptors
    /// denoting acceptance of a value.
    Accepted(Instance, Accepted),

    /// `REJECT` is sent from an acceptor in reply to a proposer
    /// when a ballot is being proposed or seen in an `ACCEPT` message that
    /// preceeds the last promised value from the acceptor.
    Reject(Instance, Reject),

    /// Request sent to a random node to get the latest value
    /// if the instance known to the node is behind.
    Sync(Instance),

    /// Response to a sync request from another peer
    Catchup(Instance, Value),
}

#[derive(Debug)]
/// Error encountered during deserialization
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
    pub fn deserialize(buf: &[u8]) -> Result<MultiPaxosMessage, DeserializeError> {
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
                Ok(MultiPaxosMessage::Prepare(inst, Prepare(proposal)))
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
                            Some((bal, Vec::from(val).into()))
                        }
                    }
                };

                Ok(MultiPaxosMessage::Promise(
                    inst,
                    Promise(proposal, last_accepted),
                ))
            }
            WhichMsg::Accept(accept) => {
                let accept = accept?;
                let proposal = { deserialize_ballot(accept.borrow().get_proposal()?) };
                let value = accept.get_value()?;
                Ok(MultiPaxosMessage::Accept(
                    inst,
                    Accept(proposal, value.to_vec().into()),
                ))
            }
            WhichMsg::Accepted(accepted) => {
                let accepted = accepted?;
                let proposal = { deserialize_ballot(accepted.borrow().get_proposal()?) };
                let value = accepted.get_value()?;
                Ok(MultiPaxosMessage::Accepted(
                    inst,
                    Accepted(proposal, value.to_vec().into()),
                ))
            }
            WhichMsg::Reject(reject) => {
                let reject = reject?;
                let proposal = { deserialize_ballot(reject.borrow().get_proposal()?) };
                let promised = { deserialize_ballot(reject.borrow().get_promised()?) };
                Ok(MultiPaxosMessage::Reject(inst, Reject(proposal, promised)))
            }
            WhichMsg::Sync(sync_req) => {
                let _ = sync_req?;
                Ok(MultiPaxosMessage::Sync(inst))
            }
            WhichMsg::Catchup(catchup) => {
                let catchup = catchup?;
                let value = catchup.get_value()?;
                Ok(MultiPaxosMessage::Catchup(inst, value.to_vec().into()))
            }
        }
    }

    pub fn serialize<W>(self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        let mut builder = Builder::new(HeapAllocator::new());

        match self {
            MultiPaxosMessage::Prepare(inst, Prepare(proposal)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let prepare = msg.borrow().init_prepare();
                let ballot = prepare.init_proposal();
                serialize_ballot(proposal, ballot);
            }
            MultiPaxosMessage::Promise(inst, Promise(proposal, last_accepted)) => {
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
            MultiPaxosMessage::Accept(inst, Accept(proposal, value)) => {
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
            MultiPaxosMessage::Accepted(inst, Accepted(proposal, value)) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut accepted = msg.borrow().init_accepted();
                {
                    let ballot = accepted.borrow().init_proposal();
                    serialize_ballot(proposal, ballot);
                }
                accepted.set_value(&value);
            }
            MultiPaxosMessage::Reject(inst, Reject(proposal, promised)) => {
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
            MultiPaxosMessage::Sync(inst) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                msg.borrow().init_sync();
            }
            MultiPaxosMessage::Catchup(inst, value) => {
                let mut msg = builder.init_root::<messages_capnp::paxos_message::Builder>();
                msg.set_instance(inst);
                let mut catchup = msg.borrow().init_catchup();
                catchup.set_value(&value);
            }
        }

        write_message(write, &builder)
    }
}

/// Message sent over the network
pub struct NetworkMessage {
    /// Address of the receiptient or destination of the message
    pub address: SocketAddr,
    /// The message
    pub message: MultiPaxosMessage,
}

/// Message sent to a peer node in the cluster
pub struct ClusterMessage {
    /// Address of the receiptient or destination of the message
    pub peer: NodeId,
    /// The message
    pub message: MultiPaxosMessage,
}
