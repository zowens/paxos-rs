use std::cmp::min;
use std::io;
use std::time::Duration;
use futures::{Async, Poll, Stream};
use futures::task;
use futures_timer::Interval;
use super::Instance;
use rand::{thread_rng, Rng};
use algo::ProposerMsg;

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
    type Stream = Interval;

    fn interval(&mut self, delay: Duration) -> Interval {
        Interval::new(delay)
    }
}

/// Timer that will resend a message to the downstream peers in order
/// to drive consensus.
pub struct RetransmitTimer<S: Scheduler> {
    scheduler: S,
    msg: Option<(Instance, ProposerMsg, S::Stream)>,
    parked_receive: Option<task::Task>,
}

impl<S> RetransmitTimer<S>
where
    S: Scheduler,
{
    pub fn new(scheduler: S) -> RetransmitTimer<S> {
        RetransmitTimer {
            scheduler,
            msg: None,
            parked_receive: None,
        }
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.msg = None;
    }

    /// Schedules a message for resend
    pub fn schedule(&mut self, inst: Instance, msg: ProposerMsg) {
        trace!("Scheduling retransmit");
        self.msg = Some((
            inst,
            msg,
            self.scheduler.interval(Duration::from_millis(1000)),
        ));

        // notify any parked receive
        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }
}

impl<S: Scheduler> Stream for RetransmitTimer<S> {
    type Item = (Instance, ProposerMsg);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<(Instance, ProposerMsg)>, io::Error> {
        match self.msg.as_mut() {
            Some(&mut (inst, ref msg, ref mut f)) => match f.poll()? {
                Async::Ready(Some(())) => Ok(Async::Ready(Some((inst, msg.clone())))),
                Async::Ready(None) => unreachable!("Infinite stream from Scheduler terminated"),
                Async::NotReady => Ok(Async::NotReady),
            },
            None => {
                self.parked_receive = Some(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}

/// Timer that allows the node to re-enter Phase 1 in order to
/// drive resolution with a higher ballot.
pub struct InstanceResolutionTimer<S: Scheduler> {
    scheduler: S,
    backoff_ms: u64,
    stream: Option<(Instance, S::Stream)>,
    parked_receive: Option<task::Task>,
}

impl<S: Scheduler> InstanceResolutionTimer<S> {
    pub fn new(scheduler: S) -> InstanceResolutionTimer<S> {
        InstanceResolutionTimer {
            scheduler,
            backoff_ms: RESOLUTION_STARTING_MS,
            stream: None,
            parked_receive: None,
        }
    }

    /// Schedules a timer to start Phase 1 when a node receives an ACCEPT
    /// message (Phase 2b) and does not hear an ACCEPTED message from a
    /// quorum of acceptors.
    pub fn schedule_timeout(&mut self, inst: Instance) {
        trace!("Scheduling PREPARE takeover timeout");

        // TODO: do we want to add some Jitter?
        self.stream = Some((
            inst,
            self.scheduler
                .interval(Duration::from_millis(RESOLUTION_SILENCE_TIMEOUT)),
        ));

        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }

    /// Schedules a timer to schedule retry of the round with a higher ballot.
    pub fn schedule_retry(&mut self, inst: Instance) {
        trace!("Scheduling retry of PREPARE phase");
        self.backoff_ms = min(self.backoff_ms * 2, RESOLUTION_MAX_MS);

        let jitter_retry_ms = thread_rng().gen_range(RESOLUTION_STARTING_MS, self.backoff_ms + 1);
        self.stream = Some((
            inst,
            self.scheduler
                .interval(Duration::from_millis(jitter_retry_ms)),
        ));

        if let Some(task) = self.parked_receive.take() {
            task.notify();
        }
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.backoff_ms = RESOLUTION_STARTING_MS;
        self.stream = None;
    }
}

impl<S: Scheduler> Stream for InstanceResolutionTimer<S> {
    type Item = Instance;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Instance>, io::Error> {
        match self.stream.as_mut() {
            Some(&mut (inst, ref mut f)) => match f.poll()? {
                Async::Ready(Some(())) => Ok(Async::Ready(Some(inst))),
                Async::Ready(None) => unreachable!("Infinite stream from Scheduler terminated"),
                Async::NotReady => Ok(Async::NotReady),
            },
            None => {
                self.parked_receive = Some(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}

/// Timer stream for periodic synchronization with a peer.
pub struct RandomPeerSyncTimer<S: Scheduler> {
    interval: S::Stream,
}

impl<S: Scheduler> RandomPeerSyncTimer<S> {
    pub fn new(mut scheduler: S) -> RandomPeerSyncTimer<S> {
        RandomPeerSyncTimer {
            interval: scheduler.interval(Duration::from_millis(SYNC_TIME_MS)),
        }
    }
}

impl<S: Scheduler> Stream for RandomPeerSyncTimer<S> {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<()>, io::Error> {
        self.interval.poll()
    }
}
