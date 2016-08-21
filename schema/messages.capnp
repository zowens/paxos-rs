@0x8536a60dcae15982;

struct Acceptor {
    address @0 :Text;
}

# TODO: Generic over value type
struct PaxosMessage {
    # In an implementation, there will be a leader process that orchestrates
    # a ballot.  The ballot b leader performs actions Phase1a(b) and
    # Phase2a(b).  The Phase1a(b) action sends a phase 1a struct (a struct
    # m with m.type = "1a") that begins ballot b.
    struct Prepare {
        ballot @0 :UInt64;
    }

    # Upon receipt of a ballot b phase 1a struct, acceptor a can perform a
    # Phase1b(a) action only if b > maxBal[a].  The action sets maxBal[a] to
    # b and sends a phase 1b struct to the leader containing the values of
    # maxVBal[a] and maxVal[a].
    struct Promise {
        struct LastAccepted {
            ballot @0 :UInt64;
            value  @1 :Data;
        }

        acceptor @0 :Acceptor;
        ballot @1 :UInt64;
        union {
            noneAccepted @2 :Void;
            lastAccepted @3 :LastAccepted;
        }
    }

    # The Phase2a(b, v) action can be performed by the ballot b leader if two
    # conditions are satisfied: (i) it has not already performed a phase 2a
    # action for ballot b and (ii) it has received ballot b phase 1b structs
    # from some quorum Q from which it can deduce that the value v is safe at
    # ballot b.  These enabling conditions are the first two conjuncts in the
    # definition of Phase2a(b, v).  This second conjunct, expressing
    # condition (ii), is the heart of the algorithm.  To understand it,
    # observe that the existence of a phase 1b struct m in msgs implies that
    # m.mbal is the highest ballot number less than m.bal in which acceptor
    # m.acc has or ever will cast a vote, and that m.mval is the value it
    # voted for in that ballot if m.mbal # -1.  It is not hard to deduce from
    # this that the second conjunct implies that there exists a quorum Q such
    # that ShowsSafeAt(Q, b, v)
    #
    # The action sends a phase 2a struct that tells any acceptor a that it
    # can vote for v in ballot b, unless it has already set maxBal[a]
    # greater than b (thereby promising not to vote in ballot b).
    struct Accept {
        ballot   @0 :UInt64;
        value    @1 :Data;
    }

    # The Phase2b(a) action is performed by acceptor a upon receipt of a
    # phase 2a struct.  Acceptor a can perform this action only if the
    # struct is for a ballot number greater than or equal to maxBal[a].  In
    # that case, the acceptor votes as directed by the phase 2a struct,
    # setting maxBVal[a] and maxVal[a] to record that vote and sending a
    # phase 2b struct announcing its vote.  It also sets maxBal[a] to the
    # struct's.  ballot number
    struct Accepted {
        acceptor @0 :Acceptor;
        ballot   @1 :UInt64;
        value    @2 :Data;
    }


    union {
        prepare  @0 :Prepare;
        promise  @1 :Promise;
        accept   @2 :Accept;
        accepted @3 :Accepted;
    }
}
