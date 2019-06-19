## 0.1.1

- Added types: timestamp, timeuuid and counter.
- Fixed a bug where it would crash on selecting and inserting a null field.

## 0.1.0

- *BREAKING CHANGES*:
  - `PageRows` implements `Page<Row>` from `package:page`.
  - `CassandraClient.close` removed, only `CassandraPool` needs it.
  - Renamed `CassandraClient` -> `Client`.
  - Renamed `CassandraPool` -> `Cluster` and updated `connect`.
  - Renamed `DataClass` -> `RawType`, `DataType` -> `Type`, `TypedValue` -> `Value`.
  - Renamed `RowsPage` -> `ResultPage`.

- Query result pagination. 

## 0.0.1

- Basic client.
