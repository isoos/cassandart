import 'dart:convert';

import 'package:cassandart/cassandart.dart';
import 'package:http/http.dart' as http;

main() async {
  final hc = http.Client();
  final client = await Cluster.connect(
    ['localhost:9042'],
    authenticator: PasswordAuthenticator('cassandra', 'cassandra'),
  );

  await client.execute('CREATE KEYSPACE IF NOT EXISTS foo '
      'WITH REPLICATION = { '
      "'class' : 'NetworkTopologyStrategy', 'DC1' : 2 }");

  await client.execute(
      'CREATE TABLE IF NOT EXISTS foo.tbl (id text PRIMARY KEY, content TEXT, category TEXT, rankz INT)');

  await client.execute(
    'INSERT INTO foo.tbl (id, content, category, rankz) VALUES (:id, :content, :category, :rankz)',
    values: {
      'id': 'cake',
      'category': 'food',
      'rankz': Value.int32(1202),
      'content':
          'Cake is a form of sweet food made from flour, sugar, and other ingredients, that is usually baked.',
    },
    consistency: Consistency.one,
  );

  await client.execute(
    'INSERT INTO foo.tbl (id, content, category, rankz) VALUES (:id, :content, :category, :rankz)',
    values: {
      'id': 'soup',
      'category': 'food',
      'rankz': Value.int32(2011),
      'content':
          'Soup is a primarily liquid food, generally served warm or hot (but may be cool or cold), '
              'that is made by combining ingredients of meat or vegetables with stock, or water. It is not baked.',
    },
    consistency: Consistency.one,
  );

  final rscreate = await hc.put(
    Uri.parse('http://localhost:9200/foo'),
    headers: {
      'content-type': 'application/json',
    },
    body: json.encode({
      'settings': {
        'index': {
          'sort.field': 'rankz',
          'sort.order': 'desc',
          'analysis': {
            'filter': {},
            'analyzer': {
              'ngram_analyzer': {
                'filter': ['lowercase'],
                'tokenizer': 'ngram_tokenizer'
              },
            },
            'tokenizer': {
              'ngram_tokenizer': {
                'type': 'ngram',
                'min_gram': 2,
                'max_gram': 3,
              }
            }
          }
        }
      },
      'mappings': {
        'tbl': {
          // 'discover': '.*',
          'properties': {
            'rankz': {
              'type': 'integer',
              'cql_collection': 'singleton',
            },
            'content': {
              'type': 'text',
              'cql_collection': 'singleton',
              'fields': {
                'keyword': {'type': 'keyword'},
                'ngram': {
                  'type': 'text',
                  'analyzer': 'ngram_analyzer',
//                  'search_analyzer': 'edge_ngram_search_analyzer'
                }
              }
            },
          },
        },
      },
    }),
  );
  print(rscreate.body);

  await Future.delayed(Duration(seconds: 1));

  final rs = await hc.get(
      Uri.parse('http://localhost:9200/foo/_search?pretty'));
  print(rs.body);

  await client.close();
  hc.close();
}
