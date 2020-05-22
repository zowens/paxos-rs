use crate::multi_paxos::Slot;
use bytes::Bytes;

/// `ReplicatedState` is a state machine that applies value synchronously. The
/// value is replicated with `MultiPaxos`.
pub trait ReplicatedState {
    /// Apply a value to the state machine
    fn apply_value(&mut self, slot: Slot, command: Bytes);

    // TODO: need log semantics support
    /// Snapshot the value within
    fn snapshot(&self) -> Option<(Slot, Bytes)>;
}
