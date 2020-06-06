use crate::multi_paxos::Slot;
use bytes::Bytes;

/// `ReplicatedState` is a state machine that applies value synchronously. The
/// value guaranteed to be replicaed once the `apply_value` function is invoked.
pub trait ReplicatedState {
    /// Apply a value to the state machine. Values are applied in slot order.
    fn apply_value(&mut self, slot: Slot, command: Bytes);
}
