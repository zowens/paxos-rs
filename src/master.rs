use std::io;
use futures::{Stream, Poll, Async};
use timer::{Scheduler, InstanceResolutionTimer};
use super::Instance;

pub enum Action {
    Prepare(Instance),
}


pub struct Masterless<S: Scheduler> {
    prepare_timer: InstanceResolutionTimer<S>,
}

impl<S: Scheduler> Masterless<S> {
    pub fn new(scheduler: S) -> Masterless<S> {
        Masterless {
            prepare_timer: InstanceResolutionTimer::new(scheduler),
        }
    }

    pub fn next_instance(&mut self) {
        self.prepare_timer.reset();
    }

    pub fn on_reject(&mut self, inst: Instance) {
        self.prepare_timer.schedule_retry(inst);
    }

    pub fn on_accept(&mut self, inst: Instance) {
        self.prepare_timer.schedule_timeout(inst);
    }

    #[cfg(test)]
    pub fn prepare_timer(&self) -> &InstanceResolutionTimer<S> {
        &self.prepare_timer
    }
}

impl<S: Scheduler> Stream for Masterless<S> {
    type Item = Action;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Action>, io::Error> {
        let timer = try_ready!(self.prepare_timer.poll());
        Ok(Async::Ready(timer.map(Action::Prepare)))
    }
}
