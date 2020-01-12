import 'dart:io';

import 'package:test/test.dart';

import 'package:cassandart/cassandart.dart';

void main() {
  group('A group of tests', () {
    Cluster client;

    setUpAll(() async {
      client = await Cluster.connect(
        ['localhost:9042'],
        authenticator: PasswordAuthenticator('cassandra', 'cassandra'),
      );
    });

    tearDownAll(() async {
      // TODO: Teardown does not end?
      await client?.close();
    });


    test('query cluster name', () async {
      final page = await client.query('SELECT cluster_name FROM system.local');
      expect(page.isLast, isTrue);
      expect(page.items.single.values, ['Test Cluster']);
    });

    test('get other peer ips', () async {
      final page = await client.query('SELECT peer FROM system.peers');
      expect(page.isLast, isTrue);
      final newIPs = page.items.map((row) => row.values[0] as InternetAddress);
      print(newIPs);
    });

    test('sleep and test', () async {
      sleep(Duration(seconds: 15));
      final page = await client.query('SELECT cluster_name FROM system.local');
      expect(page.isLast, true);
    });

    test('query index ranges', () async {
      final page = await client.query('SELECT tokens FROM system.local');
      expect(page.isLast, true);
      final vals = page.items[0].values[0] as Set;
      print(int.parse(vals.first as String)); // Different for different nodes
    });

  });
}
