part of 'cassandart_impl.dart';

class CassandraPool implements CassandraClient {
  final Authenticator _authenticator;
  final _connections = <_Connection>[];
  int _connectionCursor = 0;

  CassandraPool._(this._authenticator);

  static Future<CassandraPool> connect({
    @required List<String> hostPorts,
    @required Authenticator authenticator,
  }) async {
    final client = new CassandraPool._(authenticator);
    for (String hostPort in hostPorts) {
      await client._connect(hostPort);
    }
    return client;
  }

  Future close() async {
    while (_connections.isNotEmpty) {
      await _connections.removeLast().close();
    }
  }

  @override
  Future execute(
    String query, {
    Consistency consistency,
    /* List | Map */
    values,
  }) {
    return _withConnection(
        (c) => c._protocol.execute(query, consistency, values));
  }

  @override
  Future<RowsPage> query(
    String query, {
    Consistency consistency,
    /* List | Map */
    values,
    int pageSize,
    Uint8List pagingState,
  }) {
    final q = new _Query(query, consistency, values, pageSize, pagingState);
    final body = buildQuery(
      query: query,
      consistency: consistency,
      values: values,
      pageSize: pageSize,
      pagingState: pagingState,
    );
    return _withConnection((c) => c._protocol.query(this, q, body));
  }

  Future _connect(String hostPort) async {
    final c = await _Connection.open(
      hostPort: hostPort,
      authenticator: _authenticator,
    );
    _connections.add(c);
  }

  Future<R> _withConnection<R>(Future<R> body(_Connection c)) async {
    if (_connectionCursor >= _connections.length) {
      _connectionCursor = 0;
    }
    _connectionCursor++;
    if (_connectionCursor >= _connections.length) {
      _connectionCursor = 0;
    }
    final c = _connections[_connectionCursor];
    return await body(c);
  }
}

class _Connection {
  final String hostPort;
  final FrameProtocol _protocol;

  _Connection._(this.hostPort, this._protocol);

  static Future<_Connection> open({
    @required String hostPort,
    @required Authenticator authenticator,
  }) async {
    final host = hostPort.split(':').first;
    final port = int.parse(hostPort.split(':').last);
    final socket = await Socket.connect(host, port);
    final frameHandler =
        new FrameProtocol(new FrameSink(socket), parseFrames(socket));
    await frameHandler.start(authenticator);
    return new _Connection._(hostPort, frameHandler);
  }

  Future close() async {
    await _protocol.close();
  }
}
