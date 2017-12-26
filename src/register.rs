use std::rc::Rc;
use std::cell::RefCell;
use algo::Value;
use super::Instance;
use statemachine::ReplicatedState;

/// Replicated mutable value register
#[derive(Clone)]
pub struct Register {
    value: Rc<RefCell<Option<Value>>>,
}

impl Default for Register {
    fn default() -> Register {
        Register {
            value: Rc::new(RefCell::new(None)),
        }
    }
}

impl ReplicatedState for Register {
    fn apply_value(&mut self, instance: Instance, value: Value) {
        info!(
            "[RESOLUTION] with value at instance {}: {:?}",
            instance,
            value
        );

        let mut v = self.value.borrow_mut();
        *v = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<Value> {
        self.value.borrow().clone()
    }
}
