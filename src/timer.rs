use std::io;
use std::time::Duration;
use futures::Stream;
use futures_timer::Interval;

/// Timing scheduler that can generate a stream of events on a fixed delay.
pub trait Scheduler: Clone {
    /// Stream emitting periodic values
    type Stream: Stream<Item = (), Error = io::Error>;

    /// Periodically emits a value into the stream after a delay
    fn interval(&mut self, delay: Duration) -> Self::Stream;
}

/// Scheduler that utilizes a timeout thread for scheduling
#[derive(Clone, Default)]
pub struct FuturesScheduler;

impl Scheduler for FuturesScheduler {
    type Stream = Interval;

    fn interval(&mut self, delay: Duration) -> Interval {
        Interval::new(delay)
    }
}
