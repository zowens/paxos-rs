use super::Instance;
use paxos::Ballot;
use std::marker::PhantomData;
use value::Value;

pub struct State<V: Value> {
    pub instance: Instance,
    pub current_value: Option<V>,
    pub promised: Option<Ballot>,
    pub accepted: Option<(Ballot, V)>,
}

impl<V: Value> Default for State<V> {
    fn default() -> State<V> {
        State {
            instance: 0,
            current_value: None,
            promised: None,
            accepted: None,
        }
    }
}

pub struct StateHandler<V>(PhantomData<V>);

impl<V: Value> StateHandler<V> {
    pub fn new() -> StateHandler<V> {
        StateHandler(PhantomData)
    }

    pub fn load(&mut self) -> Option<State<V>> {
        // TODO: implement
        None
    }

    pub fn persist(&mut self, _state: State<V>) {
        // TODO: implement
    }
}
