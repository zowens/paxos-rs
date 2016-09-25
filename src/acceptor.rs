use super::{Ballot, Slot, CommandSet};
use core::cmp::max;

pub struct Acceptor<C: Clone> {
    ballot: Option<Ballot>,
    accepted: CommandSet<C>,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum PromiseResult {
    Promised(Ballot),
    Preempted(Ballot),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum AcceptResult {
    Accepted(Ballot, Slot)
}

impl<C> Acceptor<C>
    where C: Clone
{
    // TODO: persisted values from disk
    pub fn new() -> Acceptor<C> {
        Acceptor {
            ballot: None,
            accepted: CommandSet::new(),
        }
    }

    pub fn last_promised(&self) -> Option<Ballot> {
        self.ballot.clone()
    }

    pub fn accepted_in_slot<'a>(&'a self, s: Slot) -> Option<(Ballot, &'a C)> {
        self.accepted.get(s)
    }

    pub fn prepare(&mut self, b: Ballot) -> PromiseResult {
        // ballot must be > current ballot OR no ballot has been promised
        let can_promise = self.ballot.as_ref().map(|cb| cb.lt(&b)).unwrap_or(true);

        // send a promise to the leader NOT to accept anything < ballot
        // b according to the protocol, or send a NACK when we have already
        // promised a ballot greater than ballot b
        if can_promise {
            self.ballot = Some(b.clone());
            PromiseResult::Promised(b)
        } else {
            PromiseResult::Preempted(self.ballot.clone().unwrap())
        }
    }

    pub fn accept(&mut self, b: Ballot, s: Slot, c: C) -> AcceptResult {
        // update the acceptor's ballot if necessary
        let new_ballot = max(Some(b.clone()), self.ballot.clone()).unwrap();
        self.ballot = Some(new_ballot.clone());

        // accept is an imperative
        self.accepted.add(s.clone(), b.clone(), c);
        AcceptResult::Accepted(new_ballot, s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{Ballot, Node};

    macro_rules! node {
        ($e:expr ) => (
            Node($e.into())
        )
    }

    macro_rules! ballot {
        ($bv:expr, $nd:expr) => (
            Ballot($bv, node!($nd))
        )
    }

    #[test]
    fn test_prepare() {
        let mut acceptor = Acceptor::<u8>::new();
        assert_eq!(None, acceptor.last_promised());

        // prepare with ballot 10
        {
            let response = acceptor.prepare(ballot!(10, "abc"));
            assert_eq!(PromiseResult::Promised(ballot!(10, "abc")), response);
            assert_eq!(ballot!(10, "abc"), acceptor.last_promised().unwrap());
        }

        // prepare with ballot 11
        {
            let response = acceptor.prepare(ballot!(11, "abc"));
            assert_eq!(PromiseResult::Promised(ballot!(11, "abc")), response);
            assert_eq!(ballot!(11, "abc"), acceptor.last_promised().unwrap());
        }

        // prepare with ballot 5 should yield NACK
        {
            let response = acceptor.prepare(ballot!(5, "abc"));
            assert_eq!(PromiseResult::Preempted(ballot!(11, "abc")), response);
            assert_eq!(ballot!(11, "abc"), acceptor.last_promised().unwrap());
        }
    }

    #[test]
    fn test_accept() {
        let mut acceptor = Acceptor::new();

        // acceptor is told to accept a value
        {
            let response = acceptor.accept(ballot!(5, ""), 2.into(), 7);
            assert_eq!(AcceptResult::Accepted(ballot!(5, ""), 2.into()), response);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(5, ""), &7u8), acceptor.accepted_in_slot(2.into()).unwrap());
        }

        // acceptor is told to accept an older value
        {
            let response = acceptor.accept(ballot!(1, ""), 1.into(), 4);
            assert_eq!(AcceptResult::Accepted(ballot!(5, ""), 1.into()), response);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(1, ""), &4u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor is told to accept a newer value for slot
        {
            let response = acceptor.accept(ballot!(2, ""), 1.into(), 5);
            assert_eq!(AcceptResult::Accepted(ballot!(5, ""), 1.into()), response);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(2, ""), &5u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor ignores slot update for old value for slot
        {
            let response = acceptor.accept(ballot!(1, ""), 1.into(), 4);
            assert_eq!(AcceptResult::Accepted(ballot!(5, ""), 1.into()), response);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(2, ""), &5u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor updates last promised
        {
            let response = acceptor.accept(ballot!(10, ""), 7.into(), 3);
            assert_eq!(AcceptResult::Accepted(ballot!(10, ""), 7.into()), response);
            assert_eq!(ballot!(10, ""), acceptor.last_promised().unwrap());
        }
    }
}
