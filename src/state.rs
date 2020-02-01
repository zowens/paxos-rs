use super::Instance;
use crate::paxos::Ballot;
use bytes::Bytes;

pub struct State {
    pub instance: Instance,
    pub current_value: Option<Bytes>,
    pub promised: Option<Ballot>,
    pub accepted: Option<(Ballot, Bytes)>,
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

pub struct StateHandler {
    _private: (),
}

impl StateHandler {
    pub fn new() -> StateHandler {
        StateHandler{
            _private: (),
        }
    }

    pub fn load(&mut self) -> Option<State> {
        None
    }

    pub fn persist(&mut self, _state: State) {
        // TODO: implement
    }
}
