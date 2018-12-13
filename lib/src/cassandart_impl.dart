import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:buffer/buffer.dart';
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
  Future<Uint8List> respond(Uint8List challenge);
}

class PasswordAuthenticator implements Authenticator {
  final String username;
  final String password;
  PasswordAuthenticator(this.username, this.password);

  @override
  Future<Uint8List> respond(Uint8List challenge) async {
    BytesBuilder build = new BytesBuilder(copy: false);
    build.addByte(0);
    build.add(utf8.encode(username));
    build.addByte(0);
    build.add(utf8.encode(password));
    return castBytes(build.toBytes());
  }
}
