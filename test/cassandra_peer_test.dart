import 'package:test/test.dart';

import 'package:cassandart/cassandart.dart';

void main() {
  group('A group of tests', () {
    Cluster client;

    setUpAll(() async {
      client = await Cluster.connect(
        ['remote-cassandra-server:9042'],
        authenticator: PasswordAuthenticator('cassandra', 'cassandra'),
      );
    });

    tearDownAll(() async {
      await client?.close();
    });


    test('query cluster name', () async {
      final page = await client.query('SELECT cluster_name FROM system.local');
      expect(page.isLast, isTrue);
      expect(page.items.single.values, ['Test Cluster']);
    });

    test('get other peer ips', () async {
      final page = await client.query('SELECT peer FROM system.peers');
      // TODO: error Decode of RawType.inet not implemented.
      expect(page.isLast, isTrue);
    });

  });
}
