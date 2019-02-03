use super::Instance;
use statemachine::ReplicatedState;
use std::sync::{Arc, RwLock};
use value::BytesValue;

/// Replicated mutable value register
#[derive(Clone)]
pub struct Register {
    value: Arc<RwLock<Option<BytesValue>>>,
}

impl Register {
    pub fn new() -> Register {
        Register {
            value: Arc::new(RwLock::new(None)),
        }
    }
}

impl ReplicatedState for Register {
    // TODO: generic value type
    type Command = BytesValue;

    fn apply_value(&mut self, instance: Instance, value: BytesValue) {
        info!(
            "[RESOLUTION] with value at instance {}: {:?}",
            instance, value
        );

        let mut val = self.value.write().unwrap();
        *val = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<BytesValue> {
        let val = self.value.read().unwrap();
        val.clone()
    }
}
