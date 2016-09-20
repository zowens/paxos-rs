use super::{Messenger, Ballot, Slot, CommandSet, PaxosMessage};
use core::cmp::max;

pub struct Acceptor<M, C: Clone> {
    messenger: M,
    ballot: Option<Ballot>,
    accepted: CommandSet<C>,
}

impl<M, C> Acceptor<M, C>
    where M: Messenger<C>,
          C: Clone
{
    // TODO: persisted values from disk
    pub fn new(messenger: M) -> Acceptor<M, C> {
        Acceptor {
            messenger: messenger,
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

    pub fn prepare(&mut self, b: Ballot) -> M::Response {
        // ballot must be > current ballot OR no ballot has been promised
        let can_promise = self.ballot.as_ref().map(|cb| cb.lt(&b)).unwrap_or(true);

        // send a promise to the leader NOT to accept anything < ballot
        // b according to the protocol, or send a NACK when we have already
        // promised a ballot greater than ballot b
        if can_promise {
            self.ballot = Some(b.clone());
            self.messenger.send(b.1.clone(), PaxosMessage::Promise(b, self.accepted.clone()))
        } else {
            self.messenger.send(b.1.clone(),
                                PaxosMessage::PromiseNack(self.ballot.as_ref().unwrap().clone()))
        }
    }

    pub fn accept(&mut self, b: Ballot, s: Slot, c: C) -> M::Response {
        // update the acceptor's ballot if necessary
        self.ballot = max(Some(b.clone()), self.ballot.clone());

        // accept is an imperative
        self.accepted.add(s.clone(), b.clone(), c);

        self.messenger.send(b.1.clone(),
                            PaxosMessage::Accepted(self.ballot.clone().unwrap_or(b), s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{Messenger, Ballot, Node, Slot, PaxosMessage};
    use self::MessengerResponse::Sent;

    enum MessengerResponse {
        Sent(Node, PaxosMessage<u8>),
    }

    struct NoopMessenger {
    }

    impl Messenger<u8> for NoopMessenger {
        type Response = MessengerResponse;
        fn send(&self, n: Node, m: PaxosMessage<u8>) -> MessengerResponse {
            MessengerResponse::Sent(n, m)
        }
    }

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
        let mut acceptor = Acceptor::new(NoopMessenger {});
        let nd = node!("abc");

        assert_eq!(None, acceptor.last_promised());

        // prepare with ballot 10
        {
            let response = acceptor.prepare(ballot!(10, "abc"));
            assert!(matches!(response,
                Sent(ref n,
                    PaxosMessage::Promise(Ballot(10, ref mn), _)) if n.eq(&nd) && mn.eq(&nd)));

            assert_eq!(ballot!(10, "abc"), acceptor.last_promised().unwrap());
        }

        // prepare with ballot 11
        {
            let response = acceptor.prepare(ballot!(11, "abc"));
            assert!(matches!(response,
                Sent(ref n,
                    PaxosMessage::Promise(Ballot(11, ref mn), _)) if n.eq(&nd) && mn.eq(&nd)));
            assert_eq!(ballot!(11, "abc"), acceptor.last_promised().unwrap());
        }

        // prepare with ballot 5 should yield NACK
        {
            let response = acceptor.prepare(ballot!(5, "abc"));
            assert!(matches!(response,
                Sent(ref n,
                    PaxosMessage::PromiseNack(Ballot(11, ref mn))) if n.eq(&nd) && mn.eq(&nd)));
            assert_eq!(ballot!(11, "abc"), acceptor.last_promised().unwrap());
        }
    }

    #[test]
    fn test_accept() {
        let mut acceptor = Acceptor::new(NoopMessenger {});
        let nd = node!("");

        // acceptor is told to accept a value
        {
            let acceptance = acceptor.accept(ballot!(5, ""), 2.into(), 7);
            assert!(matches!(acceptance,
                Sent(ref n,
                    PaxosMessage::Accepted(Ballot(5, ref bn), Slot(2))) if n.eq(&nd) && bn.eq(&nd)));
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(5, ""), &7u8), acceptor.accepted_in_slot(2.into()).unwrap());
        }

        // acceptor is told to accept an older value
        {
            acceptor.accept(ballot!(1, ""), 1.into(), 4);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(1, ""), &4u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor is told to accept a newer value for slot
        {
            acceptor.accept(ballot!(2, ""), 1.into(), 5);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(2, ""), &5u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor ignores slot update for old value for slot
        {
            acceptor.accept(ballot!(1, ""), 1.into(), 4);
            assert_eq!(ballot!(5, ""), acceptor.last_promised().unwrap());
            assert_eq!((ballot!(2, ""), &5u8), acceptor.accepted_in_slot(1.into()).unwrap());
        }

        // acceptor updates last promised
        {
            acceptor.accept(ballot!(10, ""), 7.into(), 3);
            assert_eq!(ballot!(10, ""), acceptor.last_promised().unwrap());
        }
    }
}
