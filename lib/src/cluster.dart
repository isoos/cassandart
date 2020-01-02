part of 'cassandart_impl.dart';

/// The entry point for connecting to a cluster of Cassandra nodes.
class Cluster implements Client {
  final Authenticator _authenticator;
  final Consistency _consistency;
  final _peers = <_Peer>[];
  final _peerTokens = <_Peer, Set<int>>{};
  final _random = Random.secure();

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
    await client._collectPeers();
    await client._loadPeerTokens();
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
    // If one of the latencies is 0, latenciesSum = Inf.
    final rd = 1 - _random.nextDouble(); // 0 < rd <= 1
    double lat = 0;
    for (final entry in latencies.entries) {
      lat += 1 / entry.key;
      // If latSum = Inf, then this will be true if lat = Inf,
      // which will happen if the latency of the current peer is 0, so
      // it will select the first peer with zero latency.
      if (rd * latenciesSum <= lat) {
        return entry.value;
      }
    }
    // The above code could fail because of the imprecision of floating-point
    // numbers. In that case use a random peer as a fallback:
    return _peers[_random.nextInt(_peers.length)];
  }

  _collectPeers() async {
    final peers = await query('SELECT peer FROM system.peers');
    await for (final row in peers.asStream()) {
      final newIP = row.values[0] as InternetAddress;
      final oldPeers = _peers.map((peer) => peer.host);
      if (oldPeers.contains(newIP)) continue;
      final newPeer =
          await _Peer.connect(newIP, 9042, authenticator: _authenticator);
      _peers.add(newPeer);
    }
    _loadPeerTokens();
  }

  Future<ResultPage> _queryPeer(
      _Peer peer,
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
    return peer._sendQuery(this, q, body);
  }

  _loadPeerTokens() async {
    for (final peer in _peers) {
      final tokenPage = _queryPeer(peer, 'SELECT tokens FROM system.local');
      final tokenFirstPage = await tokenPage.asStream().first;
      final tokenData = tokenFirstPage.items[0].values[0];
      final tokens =
          (tokenData as Set).map((token) => int.parse(token as String)).toSet();
      _peerTokens[peer] = tokens;
    }
  }

  _Peer _selectTokenPeer(String hint) {
    final hash = murmur3_hash(hint);
    _Peer bestPeer;
    int bestToken;
    for(final peer in _peers) {
      // Selects the smallest token which is larger than the hash.
      final closeToken = _peerTokens[peer].reduce((a, b) {
        if(a < hash) {
          return b;
        }
        if(b < hash) {
          return a;
        }
        return min(a, b);
      });
      if(bestToken == null || closeToken < bestToken) {
        bestToken = closeToken;
        bestPeer = peer;
      }
    }
    return bestPeer;
  }

  Future executeHint(
      String query, {
        Consistency consistency,
        /* List | Map */
        values,
        String hint,
      }) {
    consistency ??= _consistency;
    final peer = _selectTokenPeer(hint);
    return peer._sendExecute(query, consistency, values);
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

  /// Average latency of the last [_latencyMaxLength] request.
  double get latency {
    if (_lastLatencies.isEmpty) {
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
    return await _trackLatency(_protocol.execute(query, consistency, values));
  }

  Future<ResultPage> _sendQuery(Client client, _Query q, Uint8List body) async {
    return await _trackLatency(_protocol.query(client, q, body));
  }

  Future<R> _trackLatency<R>(Future<R> f) async {
    final sw = Stopwatch();
    sw.start();
    final result = await f;
    sw.stop();
    if (_lastLatencies.length >= _latencyMaxLength) {
      _lastLatencies.removeLast();
    }
    _lastLatencies.addFirst(sw.elapsedMicroseconds * 1e-6);
    return result;
  }
}
