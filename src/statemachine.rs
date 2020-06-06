use crate::Slot;
use bytes::Bytes;

/// A state machine that executes sequentially applied commands.
pub trait ReplicatedState {
    /// Apply a value to the state machine.
    ///
    /// Values are applied in increasing _slot_ order. There may be holes
    /// such that there is no guarantee that _slot-1_ has been
    /// applied before _slot_.
    fn execute(&mut self, slot: Slot, command: Bytes);
}
