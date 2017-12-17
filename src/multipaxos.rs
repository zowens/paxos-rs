use messenger::Handler;
use algo::Value;
use super::Instance;

pub enum Poll<M> {
    /// Schedule delayed polling
    Schedule(M),

    /// Stop polling for this instance.
    Cancel,
}

pub trait MultiPaxos: Handler {
    /// Proposes a value and drives the behavior of the retry loop.
    fn propose_update(&mut self, value: Value) -> Poll<Instance>;

    /// Polls periodically for retransmission.
    fn poll_retransmit(&mut self, instance: Instance) -> Poll<Instance>;

    /// Polls for restarting prepare
    fn poll_restart_prepare(&mut self, instance: Instance) -> Poll<Instance>;

    /// Periodically asks a random peer for the latest value.
    fn poll_syncronization(&mut self) -> Poll<()>;
}
