# Paxos-rs 

[![Travis](https://travis-ci.org/zowens/paxos-rs.svg?branch=master)](https://travis-ci.org/zowens/paxos-rs/)

Library encapsulating Multi-Decree Paxos and variants in Rust. The core algorithm is embeddable in other systems that need a replicated state machine.

*Development Status*: Experimental (not for production use yet)

## Roadmap
- [ ] Optional Features
    - [ ] `serde` for serialize/deserialize message types
    - [ ] `futures`
- [X] Paxos Algorithm
    - [X] Core algorithm
    - [X] Distinguished Proposer
    - [ ] Flexible Quorums
    - [ ] Pipelining
    - [ ] Persistent Storage
    - [ ] Learner/observers
    - [ ] Read leases
    - [ ] Reconfiguration
    - [ ] Learners ("observers")
- [ ] Generalized Replicated State Machine
    - [ ] Mutable Register
    - [ ] Asynchronous State Machine
    - [ ] Durable Log
- [ ] Variants
    - [ ] EPaxos
    - [ ] Mencius
    - [ ] WPaxos
    - [ ] SDPaxos

## References
* [Paxos Variants](http://paxos.systems/variants.html#mencius)
* [Understanding Paxos](https://understandingpaxos.wordpress.com/)
* [Paxos Made Moderately Complex](http://paxos.systems/)
* [Flexible Quorums](https://fpaxos.github.io/)
* [WPaxos](https://muratbuffalo.blogspot.com/2017/12/wpaxos-wide-area-network-paxos-protocol.html)
