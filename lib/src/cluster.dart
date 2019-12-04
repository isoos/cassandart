part of 'cassandart_impl.dart';

/// The entry point for connecting to a cluster of Cassandra nodes.
class Cluster implements Client {
  final Authenticator _authenticator;
  final Consistency _consistency;
  final _connections = <_Connection>[];
  int _connectionCursor = 0;

  Cluster._(this._authenticator, this._consistency);

  static Future<Cluster> connect(
    List<String> hostPorts, {
    Authenticator authenticator,
    Consistency consistency,
  }) async {
    final client = Cluster._(authenticator, consistency);
    for (String hostPort in hostPorts) {
      await client._connect(hostPort);
    }
    return client;
  }

  /// Close all open connections in the cluster.
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
    consistency ??= _consistency;
    return _withConnection(
        (c) => c._protocol.execute(query, consistency, values));
  }

  @override
  Future<ResultPage> query(
    String query, {
    Consistency consistency,
    /* List | Map */
    values,
    int pageSize,
    Uint8List pagingState,
  }) {
    consistency ??= _consistency;
    final q = _Query(query, consistency, values, pageSize, pagingState);
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
      hostPort,
      authenticator: _authenticator,
    );
    _connections.add(c);
  }

  Future<R> _withConnection<R>(Future<R> body(_Connection c)) async {
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

  static Future<_Connection> open(
    String hostPort, {
    @required Authenticator authenticator,
  }) async {
    final host = hostPort.split(':').first;
    final port = int.parse(hostPort.split(':').last);
    final socket = await Socket.connect(host, port);
    final frameHandler = FrameProtocol(FrameSink(socket), parseFrames(socket));
    await frameHandler.start(authenticator);
    return _Connection._(hostPort, frameHandler);
  }

  Future close() async {
    await _protocol.close();
  }
}

class _Peer {
  /// Adddress of the connection
  final InternetAddress host;

  /// Port of the connection
  final int port;
  final FrameProtocol _protocol;

  static const int _latencyMaxLength = 100;
  Queue<double> _lastLatencies;
  // Average latency of the last [_latencyMaxLength] request.
  double get latency {
    return _lastLatencies.reduce((a, b) => a + b) / _lastLatencies.length;
  }

  _Peer._(this.host, this.port, this._protocol);
  static Future<_Peer> connectAdress(String hostPort,
      {@required Authenticator authenticator}) async {
    final host = InternetAddress(hostPort.split(':').first);
    final port = int.parse(hostPort.split(':').last);
    return await connect(host, port, authenticator: authenticator);
  }

  static Future<_Peer> connect(InternetAddress host, int port,
      {@required Authenticator authenticator}) async {
    final socket = await Socket.connect(host, port);
    final frameHandler = FrameProtocol(FrameSink(socket), parseFrames(socket));
    await frameHandler.start(authenticator);
    final peer = _Peer._(host, port, frameHandler);
    peer._lastLatencies = Queue<double>();
  }

  Future _sendExecute(String query, Consistency consistency, values) async {
    final sw = Stopwatch();
    sw.start();
    final result = await _protocol.execute(query, consistency, values);
    sw.stop();
    if(_lastLatencies.length < _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds*1e-6);
    return result;
  }

  Future _sendQuery(Client client, _Query q, Uint8List body) async {
    final sw = Stopwatch();
    sw.start();
    final result = await _trackLatency(_protocol.query(client, q, body));
    sw.stop();
    if(_lastLatencies.length < _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds*1e-6);
    return result;
  }

  Future<R> _trackLatency<R>(Future<R> f) async {
    final sw = Stopwatch();
    sw.start();
    final result = await f;
    sw.stop();
    if(_lastLatencies.length < _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds*1e-6);
    return result;
  }
}
