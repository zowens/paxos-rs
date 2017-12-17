use std::time::Duration;
use std::u64;
use rand::{thread_rng, Rng};

pub struct Backoff {
    max: Duration,
    v: Duration,
    initial: Duration,
}

fn apply_jitter(duration: Duration) -> Duration {
    let jitter_nanos = thread_rng().gen_range(0, duration.subsec_nanos());
    let jitter_secs = thread_rng().gen_range(0, duration.as_secs());
    Duration::new(jitter_secs, jitter_nanos)
}

impl Backoff {
    /// Creates an iterator that relays backoff.
    pub fn new(initial: Duration) -> Backoff {
        Backoff {
            max: Duration::from_secs(u64::MAX),
            v: initial,
            initial,
        }
    }

    /// Sets the maximum duration.
    pub fn set_max(&mut self, max: Duration) {
        self.max = max;
    }

    /// Resets the backoff to the initial backoff.
    pub fn reset(&mut self) {
        self.v = self.initial;
    }

    pub fn next_wait(&mut self) -> Duration {
        let v = self.v;
        if v > self.max {
            apply_jitter(self.max)
        } else {
            self.v = v * 2;
            apply_jitter(v)
        }
    }
}
