## 0.1.0

- *BREAKING CHANGES*:
  - `PageRows` implements `Page<Row>` from `package:page`.
  - `CassandraClient.close` removed, only `CassandraPool` needs it.
  - Renamed `CassandraClient` -> `Client`.
  - Renamed `CassandraPool` -> `Cluster` and updated `connect`.

- Query result pagination. 

## 0.0.1

- Basic client.
