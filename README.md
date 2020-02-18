# Cassandra database client in Dart 

- The package can be used with Cassandra (and Elassandra).
- It is able to handle basic types (but: no `map` yet).
- Detects and auto-connects peers.
- Auto-selects peers with lower latencies.
- Select the peer that is most likely to contain the data (use: `hint`).

**This is an experimental library and protocol, use at your own risk.**

## How to contribute

Check and follow the spec of the [wire protocol v4](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec)
before creating a PR.
