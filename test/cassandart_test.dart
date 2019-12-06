import 'dart:typed_data';

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
      await client?.close();
    });

    test('query cluster name', () async {
      final page = await client.query('SELECT cluster_name FROM system.local');
      expect(page.isLast, isTrue);
      expect(page.items.single.values, ['My Cluster']);
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

      await client.execute(
          'INSERT INTO cassandart_test.simple (id, content) VALUES (:id, :content)',
          values: {'id': 'id-3', 'content': null});

      final page1 = await client.query(
          'SELECT * FROM cassandart_test.simple WHERE id = ?',
          values: ['id-1']);
      expect(page1.items.single.values, ['id-1', 'content-1']);

      final page2 = await client.query('SELECT * FROM cassandart_test.simple');
      expect(page2.items.length, 3);

      expect(page2.items.length, 3);

      final page3 = await client.query(
          'SELECT * FROM cassandart_test.simple WHERE id = ?',
          values: ['id-3']);
      expect(page3.items.single.values, ['id-3', null]);

      final pageAll = await client.query(
        'SELECT * FROM cassandart_test.simple',
        pageSize: 2,
      );
      expect(pageAll.items.length, 2);
      expect(pageAll.isLast, false);
      final page4 = await pageAll.next();
      expect(page4.items.length, 1);
      expect(page4.isLast, true);
    });

    test('types', () async {
      await client.execute('CREATE TABLE cassandart_test.types '
          '(id text PRIMARY KEY, text_col text, int_col int, bigint_col bigint, '
          'bool_col boolean, blob_col blob, float_col float, double_col double)');
      await client.execute(
          'INSERT INTO cassandart_test.types '
          '(id, text_col, int_col, bigint_col, bool_col, blob_col, float_col, double_col) VALUES '
          '(:id, :text_col, :int_col, :bigint_col, :bool_col, :blob_col, :float_col, :double_col)',
          values: {
            'id': 'id',
            'text_col': 'text abc 123',
            'int_col': Value.int32(234353),
            'bigint_col': 573653345345,
            'bool_col': true,
            'blob_col': Uint8List.fromList([0, 2, 4, 6, 8, 10]),
            'float_col': Value.float(-12.5),
            'double_col': -1.25,
          });
      final page = await client.query(
          'SELECT * FROM cassandart_test.types WHERE id = ?',
          values: ['id']);
      expect(page.items.single.asMap(), {
        'id': 'id',
        'text_col': 'text abc 123',
        'int_col': 234353,
        'bigint_col': 573653345345,
        'bool_col': true,
        'blob_col': [0, 2, 4, 6, 8, 10],
        'float_col': -12.5,
        'double_col': -1.25,
      });
    });

    test('error in query', () async {
      expect(() => client.query('SELEECT FROM cassandart_test.simple'),
          throwsException);
    });

    test('drop keyspace', () async {
      await client.execute('DROP KEYSPACE cassandart_test;');
    });
  });
}
