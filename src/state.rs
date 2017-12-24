use algo::{Ballot, Value};
use super::Instance;

pub struct State {
    pub instance: Instance,
    pub current_value: Option<Value>,
    pub promised: Option<Ballot>,
    pub accepted: Option<(Ballot, Value)>,
}

impl Default for State {
    fn default() -> State {
        State {
            instance: 0,
            current_value: None,
            promised: None,
            accepted: None,
        }
    }
}

pub struct StateHandler {}

impl StateHandler {
    pub fn new() -> StateHandler {
        StateHandler {}
    }

    pub fn load(&mut self) -> Option<State> {
        // TODO: implement
        None
    }

    pub fn persist(&mut self, _state: State) {
        // TODO: implement
    }
}
