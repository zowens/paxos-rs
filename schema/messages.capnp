@0x8536a60dcae15982;

struct Ballot {
    id @0 :UInt64;
    nodeId @1 :UInt64;
}

# TODO: Generic over value type
struct PaxosMessage {
    struct Prepare {
        proposal @0 :Ballot;
    }

    struct Promise {
        struct LastAccepted {
            proposal @0 :Ballot;
            value  @1 :Data;
        }

        proposal @0 :Ballot;
        union {
            noneAccepted @1 :Void;
            lastAccepted @2 :LastAccepted;
        }
    }

    struct Accept {
        proposal @0 :Ballot;
        value    @1 :Data;
    }

    struct Reject {
        proposal @0 :Ballot;
        promised @1 :Ballot;
    }

    struct Accepted {
        proposal @0 :Ballot;
        value    @1 :Data;
    }

    struct Sync {
    }

    struct Catchup {
        value @0 :Data;
    }

    instance @0 :UInt64;
    union {
        prepare  @1 :Prepare;
        promise  @2 :Promise;
        accept   @3 :Accept;
        accepted @4 :Accepted;
        reject   @5 :Reject;
        sync     @6 :Sync;
        catchup  @7 :Catchup;
    }
}
