use std::rc::Rc;
use std::cell::RefCell;
use algo::BytesValue;
use super::Instance;
use statemachine::ReplicatedState;

/// Replicated mutable value register
#[derive(Clone)]
pub struct Register {
    value: Rc<RefCell<Option<BytesValue>>>,
}

impl Default for Register {
    fn default() -> Register {
        Register {
            value: Rc::new(RefCell::new(None)),
        }
    }
}

impl ReplicatedState for Register {
    // TODO: generic value type
    type Command = BytesValue;

    fn apply_value(&mut self, instance: Instance, value: BytesValue) {
        info!(
            "[RESOLUTION] with value at instance {}: {:?}",
            instance, value
        );

        let mut v = self.value.borrow_mut();
        *v = Some(value);
    }

    fn snapshot(&self, _instance: Instance) -> Option<BytesValue> {
        self.value.borrow().clone()
    }
}
