use algo::Value;
use super::Instance;
use multipaxos::ReplicatedState;

/// Replicated mutable value register
#[derive(Default)]
pub struct Register {
    value: Option<Value>,
}

impl ReplicatedState for Register {
    fn apply_value(&mut self, _instance: Instance, value: Value) {
        self.value = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<Value> {
        self.value.clone()
    }
}
