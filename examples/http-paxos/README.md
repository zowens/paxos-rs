# Example Key-Value Store with Paxos

Example server that implements HTTP transport for both clients and the replicas.

All reads and writes go through Paxos. Key are created upon an initial set.

## Running
```bash
$ cargo build --example http-paxos
$ ./target/debug/examples/http-paxos 0 &
$ ./target/debug/examples/http-paxos 1 &
$ ./target/debug/examples/http-paxos 2 &
```

Now you've started 3 nodes that are running Paxos! Nodes run on port 8080+id so ports 8080, 8081 and 8082 are used.

## Client API

The API is just a simple HTTP-based API you can use vai CURL.

| Description | Method | Path   | Request Body    | Response Codes |
| ----------- | ------ | ------ | --------------- | -------------- |
| Read value  | GET    | /{key} | X               | 200, 404       |
| Write value | POST   | /{key} | Value to be set | 204            |


### Example
```bash
# GET the "foo" key which does not exist yet
$ curl localhost:8080/foo -i
HTTP/1.1 404 Not Found
content-length: 0
date: Tue, 09 Jun 2020 19:54:19 GMT

# SET the key to a value by passing the value in the body
$ curl localhost:8080/foo -d 'hello paxos' -i
HTTP/1.1 204 No Content
x-paxos-slot: 3
content-length: 0
date: Tue, 09 Jun 2020 19:55:29 GMT

# Now run a GET again on the key to get the value
$ curl localhost:8080/foo -i
HTTP/1.1 200 OK
x-paxos-slot: 4
content-length: 11
date: Tue, 09 Jun 2020 19:56:41 GMT

hello paxos
```

The key `_paxos` is reserved.


## Paxos API

All API requests are sent via POST on the `/_paxos` path with an internal binary encoding.
