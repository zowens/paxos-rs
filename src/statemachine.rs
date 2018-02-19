use value::Value;
use super::Instance;

/// `ReplicatedState` is a state machine that applies value synchronously. The
/// value is replicated with `MultiPaxos`.
pub trait ReplicatedState {
    /// Command Value Type
    type Command: Value;

    // TODO: add "SnapshotValue" type, and "applySnapshot" func

    /// Apply a value to the state machine
    fn apply_value(&mut self, instance: Instance, command: Self::Command);

    // TODO: need log semantics support
    /// Snapshot the value within
    fn snapshot(&self, instance: Instance) -> Option<Self::Command>;
}
