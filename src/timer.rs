use std::time::Duration;
use futures::Stream;

pub trait Scheduler: Clone {
    /// Stream emitting periodic values
    type Stream: Stream<Item = (), Error = ()>;

    /// Periodically emits a value into the stream after a delay
    fn interval(&mut self, delay: Duration) -> Self::Stream;
}
