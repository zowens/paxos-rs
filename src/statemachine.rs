use algo::Value;
use super::Instance;

/// `ReplicatedState` is a state machine that applies value synchronously. The
/// value is replicated with `MultiPaxos`.
pub trait ReplicatedState {
    /// Apply a value to the state machine
    fn apply_value(&mut self, instance: Instance, value: Value);

    // TODO: need log semantics
    /// Snapshots the value
    fn snapshot(&self, instance: Instance) -> Option<Value>;
}
