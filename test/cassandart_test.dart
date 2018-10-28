import 'package:test/test.dart';

import 'package:cassandart/cassandart.dart';

void main() {
  group('A group of tests', () {
    CassandraPool client;

    setUpAll(() async {
      client = await CassandraPool.connect(
        hostPorts: ['localhost:9042'],
        authenticator: new PasswordAuthenticator('cassandra', 'cassandra'),
      );
    });

    tearDownAll(() async {
      await client.close();
    });

    test('query cluster name', () async {
      final page = await client.query('SELECT cluster_name FROM system.local');
      expect(page.isLastPage, isTrue);
      expect(page.rows.single.values, ['My Cluster']);
    });

    test('create keyspace', () async {
      await client.execute('CREATE KEYSPACE IF NOT EXISTS cassandart_test '
          'WITH REPLICATION = { '
          "'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    });

    test('simple table', () async {
      await client.execute(
          'CREATE TABLE cassandart_test.simple (id text PRIMARY KEY, content text)');

      await client.execute(
          'INSERT INTO cassandart_test.simple (id, content) VALUES (?, ?)',
          values: ['id-1', 'content-1']);

      await client.execute(
          'INSERT INTO cassandart_test.simple (id, content) VALUES (:id, :content)',
          values: {'id': 'id-2', 'content': 'content-2'});

      final page1 = await client.query(
          'SELECT * FROM cassandart_test.simple WHERE id = ?',
          values: ['id-1']);
      expect(page1.rows.single.values, ['id-1', 'content-1']);

      final page2 = await client.query('SELECT * FROM cassandart_test.simple');
      expect(page2.rows.length, 2);
    });

    test('drop keyspace', () async {
      await client.execute('DROP KEYSPACE cassandart_test;');
    });
  });
}
