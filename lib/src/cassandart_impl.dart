import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:meta/meta.dart';

part 'client.dart';
part 'collection.dart';
part 'frame_protocol.dart';
part 'frames.dart';
part 'queries.dart';
part 'serialization.dart';

abstract class CassandraClient {
  Future execute(
    String query, {
    Consistency consistency,
    /* List | Map */
    values,
  });

  Future<RowsPage> query(
    String query, {
    Consistency consistency,
    /* List | Map */
    values,
  });

  Future close();
}

enum Consistency {
  any,
  one,
  two,
  three,
  quorum,
  all,
  localQuorum,
  eachQuorum,
  serial,
  localSerial,
  localOne,
}

abstract class Authenticator {
  Future<List<int>> respond(List<int> challenge);
}

class PasswordAuthenticator implements Authenticator {
  final String username;
  final String password;
  PasswordAuthenticator(this.username, this.password);

  @override
  Future<List<int>> respond(List<int> challenge) async {
    return new CombinedListView([
      [0],
      utf8.encode(username),
      [0],
      utf8.encode(password),
    ]);
  }
}
