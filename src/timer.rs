//! Timing abstractions that implement types from the `futures` crate.
use super::Instance;
use futures::stream::{Map, MapErr};
use futures::task;
use futures::{Async, Poll, Stream};
use rand::{thread_rng, Rng};
use std::cmp::min;
use std::io;
use std::mem;
use std::time::{Duration, Instant};
use tokio_timer::{self, Interval};

/// Starting timeout for restarting Phase 1
const RESOLUTION_STARTING_MS: u64 = 5;
/// Cap on the timeout for restarting Phase 1
const RESOLUTION_MAX_MS: u64 = 3000;
/// Timeout for post-ACCEPT restarting Phase 1
const RESOLUTION_SILENCE_TIMEOUT: u64 = 5000;
/// Periodic synchronization time
const SYNC_TIME_MS: u64 = 5000;

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
    type Stream = Map<MapErr<Interval, fn(tokio_timer::Error) -> io::Error>, fn(Instant) -> ()>;

    fn interval(&mut self, delay: Duration) -> Self::Stream {
        fn map_tokio_err(e: tokio_timer::Error) -> io::Error {
            error!("Error with timer: {}", e);
            io::Error::new(io::ErrorKind::Other, "Timer error")
        }

        fn map_tokio_item(_: Instant) -> () {
            ()
        }

        let x: MapErr<Interval, fn(tokio_timer::Error) -> io::Error> =
            Interval::new(Instant::now() + delay, delay).map_err(map_tokio_err);
        x.map(map_tokio_item)
    }
}

enum TimerState<S: Scheduler, M: Clone> {
    Empty,
    Scheduled(S::Stream, M),
    Parked(task::Task),
}

impl<S: Scheduler, M: Clone> TimerState<S, M> {
    fn poll_stream(&mut self) -> Poll<Option<M>, io::Error> {
        match *self {
            TimerState::Scheduled(ref mut s, ref m) => match s.poll()? {
                Async::Ready(Some(())) => Ok(Async::Ready(Some(m.clone()))),
                Async::Ready(None) => unreachable!("Infinite stream from Scheduler terminated"),
                Async::NotReady => Ok(Async::NotReady),
            },
            // otherwise, park the current task
            // TODO: do we need to re-park?
            _ => {
                *self = TimerState::Parked(task::current());
                Ok(Async::NotReady)
            }
        }
    }

    fn reset(&mut self) {
        if let TimerState::Scheduled(..) = *self {
            *self = TimerState::Empty;
        }
    }

    fn put_message(&mut self, s: S::Stream, msg: M) {
        let mut m = TimerState::Scheduled(s, msg);
        mem::swap(self, &mut m);
        if let TimerState::Parked(t) = m {
            t.notify();
        }
    }
}

/// Timer that will resend a message to the downstream peers in order
/// to drive consensus.
pub(crate) struct RetransmitTimer<S: Scheduler, V: Clone> {
    scheduler: S,
    state: TimerState<S, (Instance, V)>,
}

impl<S, V: Clone> RetransmitTimer<S, V>
where
    S: Scheduler,
{
    pub fn new(scheduler: S) -> RetransmitTimer<S, V> {
        RetransmitTimer {
            scheduler,
            state: TimerState::Empty,
        }
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.state.reset();
    }

    /// Schedules a message for resend
    pub fn schedule(&mut self, inst: Instance, msg: V) {
        trace!("Scheduling retransmit");
        self.state.put_message(
            self.scheduler.interval(Duration::from_millis(1000)),
            (inst, msg),
        );
    }

    #[cfg(test)]
    pub fn stream(&self) -> Option<&S::Stream> {
        match self.state {
            TimerState::Scheduled(ref s, _) => Some(s),
            _ => None,
        }
    }
}

impl<S: Scheduler, V: Clone> Stream for RetransmitTimer<S, V> {
    type Item = (Instance, V);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<(Instance, V)>, io::Error> {
        self.state.poll_stream()
    }
}

/// Timer that allows the node to re-enter Phase 1 in order to
/// drive resolution with a higher ballot.
pub(crate) struct InstanceResolutionTimer<S: Scheduler> {
    scheduler: S,
    backoff_ms: u64,
    state: TimerState<S, Instance>,
}

impl<S: Scheduler> InstanceResolutionTimer<S> {
    pub fn new(scheduler: S) -> InstanceResolutionTimer<S> {
        InstanceResolutionTimer {
            scheduler,
            backoff_ms: RESOLUTION_STARTING_MS,
            state: TimerState::Empty,
        }
    }

    /// Schedules a timer to start Phase 1 when a node receives an ACCEPT
    /// message (Phase 2b) and does not hear an ACCEPTED message from a
    /// quorum of acceptors.
    pub fn schedule_timeout(&mut self, inst: Instance) {
        trace!("Scheduling PREPARE takeover timeout");

        // TODO: do we want to add some Jitter?
        self.state.put_message(
            self.scheduler
                .interval(Duration::from_millis(RESOLUTION_SILENCE_TIMEOUT)),
            inst,
        );
    }

    /// Schedules a timer to schedule retry of the round with a higher ballot.
    pub fn schedule_retry(&mut self, inst: Instance) {
        trace!("Scheduling retry of PREPARE phase");
        self.backoff_ms = min(self.backoff_ms * 2, RESOLUTION_MAX_MS);

        let jitter_retry_ms = thread_rng().gen_range(RESOLUTION_STARTING_MS, self.backoff_ms + 1);

        self.state.put_message(
            self.scheduler
                .interval(Duration::from_millis(jitter_retry_ms)),
            inst,
        );
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.backoff_ms = RESOLUTION_STARTING_MS;
        self.state.reset();
    }

    #[cfg(test)]
    pub fn stream(&self) -> Option<&S::Stream> {
        match self.state {
            TimerState::Scheduled(ref s, _) => Some(s),
            _ => None,
        }
    }
}

impl<S: Scheduler> Stream for InstanceResolutionTimer<S> {
    type Item = Instance;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Instance>, io::Error> {
        self.state.poll_stream()
    }
}

/// Timer stream for periodic synchronization with a peer.
pub(crate) struct RandomPeerSyncTimer<S: Scheduler> {
    interval: S::Stream,
}

impl<S: Scheduler> RandomPeerSyncTimer<S> {
    pub fn new(mut scheduler: S) -> RandomPeerSyncTimer<S> {
        RandomPeerSyncTimer {
            interval: scheduler.interval(Duration::from_millis(SYNC_TIME_MS)),
        }
    }

    #[cfg(test)]
    pub fn stream(&self) -> &S::Stream {
        &self.interval
    }
}

impl<S: Scheduler> Stream for RandomPeerSyncTimer<S> {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<()>, io::Error> {
        self.interval.poll()
    }
}
