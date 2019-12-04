part of 'cassandart_impl.dart';

/// The entry point for connecting to a cluster of Cassandra nodes.
class Cluster implements Client {
  final Authenticator _authenticator;
  final Consistency _consistency;
  final _peers = <_Peer>[];

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
    while (_peers.isNotEmpty) {
      await _peers.removeLast().close();
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
    final peer = _selectPeer();
    return peer._sendExecute(query, consistency, values);
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
    final peer = _selectPeer();
    return peer._sendQuery(this, q, body);
  }

  Future _connect(String hostPort) async {
    final c = await _Peer.connectAdress(
      hostPort,
      authenticator: _authenticator,
    );
    _peers.add(c);
  }

  _Peer _selectPeer() {
    final latencies = Map.fromIterables(_peers.map((p) => p.latency), _peers);
    final latenciesSum = latencies.keys.fold<double>(0, (a, b) => a + 1 / b);
    final rd = Random().nextDouble();
    double lat = 0;
    for (final entry in latencies.entries) {
      lat += 1 / entry.key;
      if (rd * latenciesSum < lat) {
        return entry.value;
      }
    }
    // if above fails becase some unknown flotng point error:
    return _peers[Random().nextInt(_peers.length)];
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
    if(_lastLatencies.length == 0) {
      return 0;
    }
    return _lastLatencies.reduce((a, b) => a + b) / _lastLatencies.length;
  }

  _Peer._(this.host, this.port, this._protocol);

  /// Connect to a server with a string address
  static Future<_Peer> connectAdress(String hostPort,
      {@required Authenticator authenticator}) async {
    final host =
        (await InternetAddress.lookup(hostPort.split(':').first)).first;
    final port = int.parse(hostPort.split(':').last);
    return await connect(host, port, authenticator: authenticator);
  }

  /// Connect to a server with ip/port address
  static Future<_Peer> connect(InternetAddress host, int port,
      {@required Authenticator authenticator}) async {
    final socket = await Socket.connect(host, port);
    final frameHandler = FrameProtocol(FrameSink(socket), parseFrames(socket));
    await frameHandler.start(authenticator);
    final peer = _Peer._(host, port, frameHandler);
    peer._lastLatencies = Queue<double>();
    return peer;
  }

  /// Closes the connection to the peer
  close() async {
    return await _protocol.close();
  }

  Future _sendExecute(String query, Consistency consistency, values) async {
    final sw = Stopwatch();
    sw.start();
    final result = await _protocol.execute(query, consistency, values);
    sw.stop();
    if (_lastLatencies.length >= _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds * 1e-6);
    return result;
  }

  Future<ResultPage> _sendQuery<R>(
      Client client, _Query q, Uint8List body) async {
    final sw = Stopwatch();
    sw.start();
    final result = await _protocol.query(client, q, body);
    sw.stop();
    if (_lastLatencies.length >= _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds * 1e-6);
    return result;
  }

  /*
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
   */
}
