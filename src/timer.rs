//! Timing abstractions that implement types from the `futures` crate.
use super::Instance;
use futures::Stream;
use rand::{thread_rng, Rng};
use std::cmp::min;
use std::time::Duration;
use std::task::{Poll, Waker, Context};
use tokio::time::{Interval, interval};
use std::pin::Pin;
use pin_project::pin_project;

/// Starting timeout for restarting Phase 1
const RESOLUTION_STARTING_MS: u64 = 5;
/// Cap on the timeout for restarting Phase 1
const RESOLUTION_MAX_MS: u64 = 3000;
/// Timeout for post-ACCEPT restarting Phase 1
const RESOLUTION_SILENCE_TIMEOUT: u64 = 5000;
/// Periodic synchronization time
const SYNC_TIME_MS: u64 = 5000;

#[pin_project]
enum TimerState<M: Clone> {
    Empty,
    Scheduled(#[pin] Interval, M),
    Parked(Waker),
}

impl<M: Clone> TimerState<M> {
    fn poll_stream(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<M>> {
        {
            use self::__TimerStateProjection::*;
            let self_pin = self.as_mut().project();
            if let Scheduled(s, m) = self_pin {
                match ready!(s.poll_next(cx)) {
                    Some(_) => return Poll::Ready(Some(m.clone())),
                    None => unreachable!("Infinite stream from scheduler terminated")
                }
            }
        }

        self.set(TimerState::Parked(cx.waker().clone()));
        Poll::Pending
    }

    fn reset(&mut self) {
        if let TimerState::Scheduled(..) = *self {
            *self = TimerState::Empty;
        }
    }

    fn put_message(&mut self, s: Interval, msg: M) {
        let waker = if let TimerState::Parked(ref waker) = *self {
            Some(waker.clone())
        } else {
            None
        };

        *self = TimerState::Scheduled(s, msg);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Timer that will resend a message to the downstream peers in order
/// to drive consensus.
#[pin_project]
pub struct RetransmitTimer<V: Clone> {
    #[pin]
    state: TimerState<(Instance, V)>,
}

impl<V: Clone> RetransmitTimer<V>
{
    /// Clears the current timer
    pub fn reset(&mut self) {
        self.state.reset();
    }

    /// Schedules a message for resend
    pub fn schedule(&mut self, inst: Instance, msg: V) {
        trace!("Scheduling retransmit");
        self.state.put_message(
            interval(Duration::from_millis(1_000)),
            (inst, msg),
        );
    }
}

impl<V: Clone> Default for RetransmitTimer<V> {
    fn default() -> RetransmitTimer<V> {
        RetransmitTimer {
            state: TimerState::Empty,
        }
    }
}

impl<V: Clone> Stream for RetransmitTimer<V> {
    type Item = (Instance, V);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<(Instance, V)>> {
        self.project().state.poll_stream(cx)
    }
}

/// Timer that allows the node to re-enter Phase 1 in order to
/// drive resolution with a higher ballot.
#[pin_project]
pub(crate) struct InstanceResolutionTimer {
    backoff_ms: u64,
    #[pin]
    state: TimerState<Instance>,
}

impl InstanceResolutionTimer {
    /// Schedules a timer to start Phase 1 when a node receives an ACCEPT
    /// message (Phase 2b) and does not hear an ACCEPTED message from a
    /// quorum of acceptors.
    pub fn schedule_timeout(&mut self, inst: Instance) {
        trace!("Scheduling PREPARE takeover timeout");

        // TODO: do we want to add some Jitter?
        self.state.put_message(
                interval(Duration::from_millis(RESOLUTION_SILENCE_TIMEOUT)),
            inst,
        );
    }

    /// Schedules a timer to schedule retry of the round with a higher ballot.
    pub fn schedule_retry(&mut self, inst: Instance) {
        trace!("Scheduling retry of PREPARE phase");
        self.backoff_ms = min(self.backoff_ms * 2, RESOLUTION_MAX_MS);

        let jitter_retry_ms = thread_rng().gen_range(RESOLUTION_STARTING_MS, self.backoff_ms + 1);

        self.state.put_message(
                interval(Duration::from_millis(jitter_retry_ms)),
            inst,
        );
    }

    /// Clears the current timer
    pub fn reset(&mut self) {
        self.backoff_ms = RESOLUTION_STARTING_MS;
        self.state.reset();
    }
}

impl Default for InstanceResolutionTimer {
    fn default() -> InstanceResolutionTimer {
        InstanceResolutionTimer {
            backoff_ms: RESOLUTION_STARTING_MS,
            state: TimerState::Empty,
        }
    }
}

impl Stream for InstanceResolutionTimer {
    type Item = Instance;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Instance>> {
        self.project().state.poll_stream(cx)
    }
}

/// Timer stream for periodic synchronization with a peer.
#[pin_project]
pub struct RandomPeerSyncTimer {
    #[pin]
    interval: Interval,
}

impl Default for RandomPeerSyncTimer {
    fn default() -> RandomPeerSyncTimer {
        RandomPeerSyncTimer {
            interval: interval(Duration::from_millis(SYNC_TIME_MS)),
        }
    }
}

impl Stream for RandomPeerSyncTimer {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        self.project().interval.poll_next(cx).map(|o| o.map(|_| ()))
    }
}
