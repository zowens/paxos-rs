use bincode;
use bytes::Bytes;
use paxos::{ReplicatedState, Slot};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot::{channel, Receiver, Sender};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    Get { request_id: u64, key: Bytes },
    Set { request_id: u64, key: Bytes, value: Bytes },
}

impl Into<Bytes> for KvCommand {
    fn into(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }
}

impl TryFrom<Bytes> for KvCommand {
    // TODO: better error handling for the parser
    type Error = ();

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&mut value).map_err(|e| {
            error!("Error deserializing key value command: {:?}", e);
        })
    }
}

struct Inner {
    values: HashMap<Bytes, Bytes>,
    pending_set: HashMap<u64, Sender<Slot>>,
    pending_get: HashMap<u64, Sender<Option<(Slot, Bytes)>>>,
}

#[derive(Clone)]
pub struct KeyValueStore {
    inner: Arc<Mutex<Inner>>,
}

impl Default for KeyValueStore {
    fn default() -> KeyValueStore {
        KeyValueStore {
            inner: Arc::new(Mutex::new(Inner {
                values: HashMap::default(),
                pending_set: HashMap::default(),
                pending_get: HashMap::default(),
            })),
        }
    }
}

impl KeyValueStore {
    pub fn register_get(&self, id: u64) -> Receiver<Option<(Slot, Bytes)>> {
        let (snd, recv) = channel();
        {
            let mut inner = self.inner.lock().unwrap();
            &inner.pending_get.insert(id, snd);
        }
        recv
    }

    pub fn register_set(&self, id: u64) -> Receiver<Slot> {
        let (snd, recv) = channel();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.pending_set.insert(id, snd);
        }
        recv
    }

    pub fn prune_listeners(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending_get.retain(|_, val| !val.is_closed());
        inner.pending_set.retain(|_, val| !val.is_closed());
    }
}

impl ReplicatedState for KeyValueStore {
    fn execute(&mut self, slot: Slot, cmd: Bytes) {
        match KvCommand::try_from(cmd) {
            Ok(KvCommand::Get { request_id, key }) => {
                let mut inner = self.inner.lock().unwrap();
                let sender = match inner.pending_get.remove(&request_id) {
                    Some(sender) => sender,
                    None => return,
                };
                match inner.values.get(&key).cloned() {
                    Some(val) => sender.send(Some((slot, val))).unwrap_or(()),
                    None => sender.send(None).unwrap_or(()),
                }
            }
            Ok(KvCommand::Set { request_id, key, value }) => {
                let mut inner = self.inner.lock().unwrap();
                inner.values.insert(key, value);
                if let Some(sender) = inner.pending_set.remove(&request_id) {
                    sender.send(slot).unwrap_or(());
                }
            }
            Err(()) => {}
        }
    }
}
