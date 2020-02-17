## 0.3.0

- Peer-selection hint handles `List<String>` primary keys.

**BREAKING CHANGES**:

- `TIMESTAMP` is now serialized from/to `DateTime` (with UTC).
- Refactored methods around frame parsing (internals).
- `Client` also has the `hint` parameter (as `Cluster` has it).

## 0.2.0

Better cluster handling:
- Automatically connect to new peers.
- Peer selection based on low latency connection.
- Peer selection based on hint to connect to primary node directly (limited to String ids).

## 0.1.2

- Updated to use `Uint8List`.

## 0.1.1

- Added types: timestamp, timeuuid and counter.
- Fixed a bug where it would crash on selecting and inserting a null field.
- Updated to Dart 2.3 standards

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
