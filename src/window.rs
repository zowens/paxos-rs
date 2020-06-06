use super::Slot;
use crate::{acceptor::Acceptor, Ballot};
use bytes::Bytes;
use std::cmp::max;
use std::ops::Range;

struct ResolvedSlot(Ballot, Bytes);

/// Tracking for open and decided slots for a paxos replica
pub struct SlotWindow {
    /// Slots that are indexed >= open_min_slot.
    ///
    /// Some slots may be decided, but the open_min_slot is quaranteed
    /// to be still undecided.
    open: Vec<Acceptor>,
    open_min_slot: Slot,
    max_promised: Option<Ballot>,

    /// Slots that have been decided.
    decided: Vec<ResolvedSlot>,

    /// Size of the phase 2 quorum
    quorum: usize,
}

impl SlotWindow {
    /// New tracker for slots
    pub fn new(quorum: usize) -> SlotWindow {
        let mut open = Vec::new();
        // add the first slot
        open.push(Acceptor::new(None, quorum));

        SlotWindow {
            open,
            open_min_slot: 0,
            max_promised: None,
            decided: Vec::new(),
            quorum,
        }
    }

    /// Mutable reference to a slot
    pub fn slot_mut(&mut self, slot: Slot) -> SlotMutRef {
        if slot < self.open_min_slot {
            assert!((slot as usize) < self.decided.len());
            let ResolvedSlot(ballot, value) = &self.decided[slot as usize];
            SlotMutRef::Resolved(*ballot, value.clone())
        } else if slot < self.open_min_slot + self.open.len() as Slot {
            let open_index = (slot - self.open_min_slot) as usize;
            assert!(open_index < self.open.len());

            {
                if let Some((bal, val)) = &self.open[open_index].resolution() {
                    return SlotMutRef::Resolved(*bal, val.clone());
                }
            }

            SlotMutRef::Open(OpenSlotMutRef {
                i: open_index,
                window: self,
            })
        } else {
            SlotMutRef::Empty(EmptySlotRef { slot, window: self })
        }
    }

    /// Opens the next slot
    pub fn next_slot(&mut self) -> OpenSlotMutRef {
        if self.open.last().is_some() && !self.open.last().unwrap().highest_value().is_some() {
            return OpenSlotMutRef {
                i: self.open_min_slot as usize + self.open.len() - 1,
                window: self,
            };
        }

        let i = self.open.len();
        self.open
            .push(Acceptor::new(self.max_promised, self.quorum));
        OpenSlotMutRef { i, window: self }
    }

    /// Iterates on slot numbers of the open window.
    ///
    /// Some slots may have been resolved, but the start of the range
    /// contains the min slot that is open.
    pub fn open_range(&self) -> Range<Slot> {
        Range {
            start: self.open_min_slot,
            end: self.open_min_slot + self.open.len() as Slot,
        }
    }

    fn fill_decisions(&mut self) {
        // find the range of resolved slots
        let last_resolved = self
            .open
            .iter()
            .take_while(|slot| slot.resolution().is_some())
            .enumerate()
            .map(|(i, _)| i)
            .last();

        // move resolved slots into the decided vector
        if let Some(i) = last_resolved {
            self.open_min_slot += (i as u64) + 1;
            let resolutions = self.open.drain(0..=i).map(|open_slot| {
                let (bal, val) = open_slot.resolution().unwrap();
                ResolvedSlot(bal, val.clone())
            });
            self.decided.extend(resolutions);
            self.fill_open_slots(self.open_min_slot);
        }
    }

    fn fill_open_slots(&mut self, max_slot: Slot) {
        if max_slot < self.open_min_slot {
            return;
        }

        let quorum = self.quorum;
        let last_promised = self.max_promised;
        self.open.extend(
            (self.open_min_slot + self.open.len() as u64..=max_slot)
                .map(|_| Acceptor::new(last_promised, quorum)),
        );
    }
}

/// Mutable reference to an open slot
pub struct OpenSlotMutRef<'a> {
    i: usize,
    window: &'a mut SlotWindow,
}

impl<'a> OpenSlotMutRef<'a> {
    pub fn slot(&self) -> Slot {
        self.i as Slot + self.window.open_min_slot
    }

    pub fn acceptor(&mut self) -> &mut Acceptor {
        &mut self.window.open[self.i]
    }
}

impl<'a> Drop for OpenSlotMutRef<'a> {
    fn drop(&mut self) {
        let acceptor_promised = self.acceptor().promised();
        self.window.max_promised = max(self.window.max_promised, acceptor_promised);
        self.window.fill_decisions();
    }
}

/// Mutable reference to a slot
pub enum SlotMutRef<'a> {
    /// Slot is unresolved
    Open(OpenSlotMutRef<'a>),
    /// Slot has not been reserved
    Empty(EmptySlotRef<'a>),
    /// Slot is resolved with a value
    Resolved(Ballot, Bytes),
}

/// Reference to an empty slot that can be filled on demand
pub struct EmptySlotRef<'a> {
    slot: Slot,
    window: &'a mut SlotWindow,
}

impl<'a> EmptySlotRef<'a> {
    /// Filts the slot as open
    pub fn fill(self) -> OpenSlotMutRef<'a> {
        self.window.fill_open_slots(self.slot);
        let i = self.slot - self.window.open_min_slot;
        OpenSlotMutRef {
            i: i as usize,
            window: self.window,
        }
    }
}

impl<'a> SlotMutRef<'a> {
    #[cfg(test)]
    pub fn unwrap_open(self) -> OpenSlotMutRef<'a> {
        match self {
            SlotMutRef::Open(open_slot_ref) => open_slot_ref,
            _ => panic!("Slot was resolved when open expected"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_empty(self) -> EmptySlotRef<'a> {
        match self {
            SlotMutRef::Empty(empty) => empty,
            _ => panic!("Slow was resolved when empty expected"),
        }
    }

    #[cfg(test)]
    pub fn unwrap_resolved(self) -> (Ballot, Bytes) {
        match self {
            SlotMutRef::Resolved(bal, value) => (bal, value),
            _ => panic!("Slot was resolved when open expected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_open_slots() {
        let mut window = SlotWindow::new(3);
        assert_eq!(0, window.open_min_slot);

        window.fill_open_slots(5);
        assert_eq!(0, window.open_min_slot);
        assert_eq!(6, window.open.len());

        window.fill_open_slots(5);
        assert_eq!(0, window.open_min_slot);
        assert_eq!(6, window.open.len());

        window.fill_open_slots(2);
        assert_eq!(0, window.open_min_slot);
        assert_eq!(6, window.open.len());

        // fake advance the window
        window.open_min_slot = 8;
        window.fill_open_slots(2);
        assert_eq!(8, window.open_min_slot);
        assert_eq!(6, window.open.len());
    }

    #[test]
    fn windows() {
        let mut window = SlotWindow::new(3);
        assert!(match window.slot_mut(0) {
            SlotMutRef::Open(_) => true,
            _ => false,
        });

        {
            window
                .slot_mut(2)
                .unwrap_empty()
                .fill()
                .acceptor()
                .resolve(Ballot(0, 0), "123".into());
        }

        assert_eq!(0, window.open_min_slot);
        assert_eq!(3, window.open.len());
        assert_eq!((0..3), window.open_range());

        {
            window
                .slot_mut(0)
                .unwrap_open()
                .acceptor()
                .resolve(Ballot(1, 1), "456".into());
        }

        assert_eq!(1, window.open_min_slot);
        assert_eq!(2, window.open.len());
        assert_eq!((1..3), window.open_range());

        {
            window
                .slot_mut(1)
                .unwrap_open()
                .acceptor()
                .resolve(Ballot(10, 3), "789".into());
        }

        assert_eq!(3, window.open_min_slot);
        assert_eq!(1, window.open.len());
        assert_eq!((3..4), window.open_range());

        {
            let (bal, val) = window.slot_mut(0).unwrap_resolved();
            assert_eq!(Ballot(1, 1), bal);
            assert_eq!("456", val);
        }

        {
            let (bal, val) = window.slot_mut(1).unwrap_resolved();
            assert_eq!(Ballot(10, 3), bal);
            assert_eq!("789", val);
        }

        {
            let (bal, val) = window.slot_mut(2).unwrap_resolved();
            assert_eq!(Ballot(0, 0), bal);
            assert_eq!("123", val);
        }
    }

    #[test]
    pub fn test_open_one() {
        let mut window = SlotWindow::new(2);
        {
            window.slot_mut(1).unwrap_empty().fill();
        }
        assert_eq!((0..2), window.open_range());

        {
            assert!(match window.slot_mut(0) {
                SlotMutRef::Open(ref mut slot) => !slot.acceptor().highest_value().is_some(),
                _ => false,
            });
        }
    }
}
