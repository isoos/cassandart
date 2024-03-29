import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:buffer/buffer.dart';
import 'package:page/page.dart';

import 'murmur3_hash.dart';

part 'cluster.dart';
part 'frame_protocol.dart';
part 'frames.dart';
part 'queries.dart';
part 'serialization.dart';

/// The execution and query context of the Cassandra client. Underlying
/// implementation may use a connection pool or just a single connection.
abstract class Client {
  /// Execute [query] with the given parameters.
  Future<void> execute(
    String query, {
    Consistency consistency,
    /* List<dynamic> | Map<String, dynamic> */
    values,
    /* String | List<String> */
    hint,
  });

  /// Execute data row [query] with the given parameters and return a page
  /// object of the results rows (and further pagination support).
  Future<ResultPage> query(
    String query, {
    Consistency? consistency,
    /* List<dynamic> | Map<String, dynamic> */
    values,
    int? pageSize,
    Uint8List? pagingState,
    /* String | List<String> */
    hint,
  });
}

/// The consistency of an operation.
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

/// Provides response to authentication challenges.
abstract class Authenticator {
  /// Responds to the authentication [challenge].
  Future<Uint8List> respond(Uint8List challenge);
}

/// Username and password based authenticator.
class PasswordAuthenticator implements Authenticator {
  final String username;
  final String password;
  PasswordAuthenticator(this.username, this.password);

  @override
  Future<Uint8List> respond(Uint8List challenge) async {
    final build = BytesBuilder(copy: false);
    build.addByte(0);
    build.add(utf8.encode(username));
    build.addByte(0);
    build.add(utf8.encode(password));
    return castBytes(build.toBytes());
  }
}
