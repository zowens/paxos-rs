use super::Instance;
use crate::statemachine::ReplicatedState;
use std::sync::{Arc, RwLock};
use bytes::Bytes;

/// Replicated mutable value register
#[derive(Clone)]
pub struct Register {
    value: Arc<RwLock<Option<Bytes>>>,
}

impl Register {
    pub fn new() -> Register {
        Register {
            value: Arc::new(RwLock::new(None)),
        }
    }
}

impl ReplicatedState for Register {
    fn apply_value(&mut self, instance: Instance, value: Bytes) {
        info!(
            "[RESOLUTION] with value at instance {}: {:?}",
            instance, value
        );

        let mut val = self.value.write().unwrap();
        *val = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<Bytes> {
        let val = self.value.read().unwrap();
        val.clone()
    }
}
