import 'dart:math';

import 'package:cassandart/cassandart.dart';
import 'package:test/test.dart';

void main() async {
  group('Table fill with hints test', () {
    late final Cluster cluster;

    setUp(() async {
      cluster = await Cluster.connect(['remote-cassandra-server:9042'],
          authenticator: PasswordAuthenticator('cassandra', 'cassandra'));
    });

    test('Drop test', () async {
      await cluster.execute('''
        DROP TABLE cassandart_fill.table_fill ;
        ''');
      await cluster.execute('''
        DROP KEYSPACE cassandart_fill ;
        ''');
    });

    test('Create test', () async {
      await cluster.execute('''
        CREATE KEYSPACE IF NOT EXISTS cassandart_fill 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        ;
        ''');
      await cluster.execute('''
        CREATE TABLE cassandart_fill.table_fill (
          id text PRIMARY KEY)
        ;
        ''');
    });

    test('Insert test', () async {
      final random = Random();
      for (int i = 0; i < 500; i++) {
        final len = random.nextInt(12) + 3;
        final chars = <int>[];
        for (int j = 0; j < len; j++) {
          chars.add(33 + random.nextInt(94));
        }
        final id = String.fromCharCodes(chars);
        await cluster.execute('''
        INSERT INTO cassandart_fill.table_fill
        (id)
        VALUES (:id)
        ;
        ''', values: {'id': id}, hint: id);
      }
    }, timeout: Timeout(Duration(seconds: 60)));
  });
}
