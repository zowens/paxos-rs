# Paxos Made with Rust

*Development Status*: Experimental

Library encapsulating Multi-Decree Paxos and variants in Rust with a builtin server using Tokio and Futures.

The implementation is largely based on [a blog post by Tom Cocagne](https://understandingpaxos.wordpress.com/) and [the accompanying code](github.com/cocagne/multi-paxos-example). I
found the explanation and code quite easy to read and understand.

This project is laregly used to gain an understanding for the subleties of Paxos and the
variants that have been described in the literature. Over time, the goal of the project is to be as useful as other embedded consensus sytems 
(such as the popular Raft implementations in Go) with the flexability afforded by Paxos.

Additionally, this library will be used for the reconfiguration of the chain in my implementations of [chain replication](https://github.com/zowens/chain-replication).

## Building and Running

Must be using Rust nightly.

```shell
cargo build --release

# start 3 Paxos servers
./target/release/server 0 &
./target/release/server 1 &
./target/release/server 2 &

# propose a value
./target/release/client propose "hello world"

# GET the current value from each of the nodes
# 
# NOTE: The read is currently not serialized
./target/release/client get
./target/release/client -n 1 get
./target/release/client -n 2 get
```

## Progress
- [ ] Paxos Algorithm
    - [X] Core algorithm
    - [ ] Persistent Storage
- [ ] Multi-Paxos
    - [X] Masterless
    - [ ] Master Leases
        - [ ] Distinguished Proposer
        - [ ] Distinguished Learner
        - [ ] Write Batching
    - [ ] Reconfiguration
        - [ ] Member-specific state machine
        - [ ] Learners ("observers")
    - [ ] Client Protocol
        - [ ] Membership-aware protocol
- [ ] Generalized Replicated State Machine
    - [X] Mutable Register
    - [ ] Asynchronous State Machine
    - [ ] Durable Log
- [ ] Variants
    - [ ] Flexible Quorums
    - [ ] EPaxos
    - [ ] Mencius
    - [ ] WPaxos
- [ ] Engineering
    - [ ] Generic command and value types
    - [ ] Embeddable Library
    - [ ] Reject for wrong instance (rather than ignore)
    - [ ] Jepsen Testing
    - [ ] Configuration of timeouts and other internals
    - [ ] UDP vs. TCP
    - [ ] Rich client library and cli

## References
* [Paxos Variants](http://paxos.systems/variants.html#mencius)
* [Understanding Paxos](https://understandingpaxos.wordpress.com/)
* [Paxos Made Moderately Complex](http://paxos.systems/)
* [Flexible Quorums](https://fpaxos.github.io/)
* [WPaxos](https://muratbuffalo.blogspot.com/2017/12/wpaxos-wide-area-network-paxos-protocol.html)
