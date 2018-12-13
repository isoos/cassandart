part of 'cassandart_impl.dart';

abstract class Opcode {
  static const int error = 0x00;
  static const int start = 0x01;
  static const int ready = 0x02;
  static const int authenticate = 0x03;
  static const int options = 0x05;
  static const int supported = 0x06;
  static const int query = 0x07;
  static const int result = 0x08;
  static const int prepare = 0x09;
  static const int execute = 0x0A;
  static const int register = 0x0B;
  static const int event = 0x0C;
  static const int batch = 0x0D;
  static const int authChallenge = 0x0E;
  static const int authResponse = 0x0F;
  static const int authSuccess = 0x10;
}

class FrameProtocol {
  final protocolVersion = 4;
  final Sink<Frame> _requestSink;
  final Stream<Frame> _responseStream;
  final _responseCompleters = <int, Completer<Frame>>{};
  final _eventController = new StreamController<Frame>();
  StreamSubscription<Frame> _responseSubscription;

  FrameProtocol(this._requestSink, this._responseStream);

  Future start(Authenticator authenticator) async {
    _responseSubscription = _responseStream.listen(_handleResponse);
    final rs = await send(Opcode.start,
        (new BodyWriter()..writeStringMap({'CQL_VERSION': '3.0.0'})).toBytes());
    if (rs.opcode == Opcode.ready) {
      return;
    }
    if (rs.opcode == Opcode.authenticate) {
      final reader = new BodyReader(rs.body);
      final className = reader.parseShortString();
      if (className == 'org.apache.cassandra.auth.PasswordAuthenticator') {
        final payload = await authenticator.respond(null);
        final body = new BodyWriter()..writeBytes(payload);
        final auth = await send(Opcode.authResponse, body.toBytes());
        _throwIfError(auth);
        if (auth.opcode == Opcode.authSuccess) {
          return;
        }
        throw new UnimplementedError(
            'Unimplemented auth handler: ${auth.opcode}');
      }
    }
    throw new UnimplementedError(
        'Unimplemented opcode handler: ${rs.opcode}');
  }

  Stream<Frame> get events => _eventController.stream;

  Future<Frame> send(int opcode, Uint8List body) {
    if (_responseSubscription == null) {
      throw new StateError('Connection is closed.');
    }
    final header = new FrameHeader(
      isRequest: true,
      protocolVersion: protocolVersion,
      isCompressed: false,
      requiresTracing: false,
      hasCustomPayload: false,
      hasWarning: false,
      streamId: _nextStreamId(),
      opcode: opcode,
      length: body == null ? 0: body.length,
    );
    final frame = new Frame(header, body);
    final c = new Completer<Frame>();
    _responseCompleters[frame.header.streamId] = c;
    _requestSink.add(frame);
    return c.future;
  }

  Future execute(String query, Consistency consistency, values) async {
    final body =
        buildQuery(query: query, consistency: consistency, values: values);
    final rs = await send(Opcode.query, body);
    _throwIfError(rs);
    if (rs.opcode == Opcode.result) {
      final br = new BodyReader(rs.body);
      final int kind = br.parseInt();
      switch (kind) {
        case _ResultKind.void$:
          return;
        case _ResultKind.rows:
          throw new UnimplementedError('Result kind $kind not supported.');
        case _ResultKind.setKeyspace:
          return;
        case _ResultKind.prepared:
          throw new UnimplementedError('Result kind $kind not supported.');
        case _ResultKind.schemaChange:
          return;
        default:
          throw new UnimplementedError('Result kind $kind not implemented.');
      }
    }
    throw new UnimplementedError('Unimplemented opcode handler: ${rs.opcode}');
  }

  Future<RowsPage> query(String query, Consistency consistency, values) async {
    final body =
        buildQuery(query: query, consistency: consistency, values: values);
    final rs = await send(Opcode.query, body);
    _throwIfError(rs);
    if (rs.opcode == Opcode.result) {
      final br = new BodyReader(rs.body);
      final int kind = br.parseInt();
      switch (kind) {
        case _ResultKind.void$:
          throw new UnimplementedError('Result kind $kind not supported.');
        case _ResultKind.rows:
          return _parseRowsBody(br);
        case _ResultKind.setKeyspace:
          throw new UnimplementedError('Result kind $kind not supported.');
        case _ResultKind.prepared:
          throw new UnimplementedError('Result kind $kind not supported.');
        case _ResultKind.schemaChange:
          throw new UnimplementedError('Result kind $kind not supported.');
        default:
          throw new UnimplementedError('Result kind $kind not implemented.');
      }
    }
    throw new UnimplementedError('Unimplemented opcode handler: ${rs.opcode}');
  }

  Future close() async {
    _requestSink.close();
    await _responseSubscription.cancel();
    _responseSubscription = null;
    await _eventController.close();
  }

  int _nextStreamId() {
    for (int i = 0; i < 32768; i++) {
      if (!_responseCompleters.containsKey(i)) {
        return i;
      }
    }
    throw new StateError('Unable to open a new stream, maximum limit reached.');
  }

  void _handleResponse(Frame frame) {
    if (frame.streamId >= 0) {
      final completer = _responseCompleters.remove(frame.streamId);
      if (completer == null) {
        // TODO: log something is not right
      }
      completer.complete(frame);
    } else {
      _eventController.add(frame);
    }
  }

  void _throwIfError(Frame frame) {
    if (frame.opcode == Opcode.error) {
      throw new ErrorResponse.parse(frame.body);
    }
  }
}

class ErrorResponse implements Exception {
  final int code;
  final String message;

  ErrorResponse(this.code, this.message);

  factory ErrorResponse.parse(Uint8List body) {
    final br = new BodyReader(body);
    final code = br.parseInt();
    final message = br.parseShortString();
    return new ErrorResponse(code, message);
  }

  @override
  String toString() => '[$code] $message';
}

abstract class _ResultKind {
  static const int void$ = 0x0001;
  static const int rows = 0x0002;
  static const int setKeyspace = 0x0003;
  static const int prepared = 0x0004;
  static const int schemaChange = 0x0005;
}

RowsPage _parseRowsBody(BodyReader br) {
  final flags = br.parseInt();
  final hasGlobalTableSpec = flags & 0x0001 != 0;
  final hasMorePages = flags & 0x0002 != 0;
  final hasNoMetadata = flags & 0x0004 != 0;
  final columnsCount = br.parseInt();
  List<int> pagingState;
  if (hasMorePages) {
    pagingState = br.parseBytes();
  }
  String globalKeyspace;
  String globalTable;
  if (hasGlobalTableSpec) {
    globalKeyspace = br.parseShortString();
    globalTable = br.parseShortString();
  }
  final columns = <Column>[];
  for (int i = 0; i < columnsCount; i++) {
    String keyspace = globalKeyspace;
    String table = globalTable;
    if (!hasGlobalTableSpec) {
      keyspace = br.parseShortString();
      table = br.parseShortString();
    }
    String column = br.parseShortString();
    final valueType = _parseValueType(br);
    columns.add(new Column(keyspace, table, column, valueType));
  }
  final rowsCount = br.parseInt();
  final rows = new List<_Row>(rowsCount);
  for (int i = 0; i < rowsCount; i++) {
    final values = new List(columnsCount);
    for (int j = 0; j < columnsCount; j++) {
      final bytes = br.parseBytes();
      values[j] = decodeData(columns[j].dataType, bytes);
    }
    rows[i] = new _Row(columns, values);
  }

  return new _RowsPage(columns, rows, !hasMorePages);
}

enum DataClass {
  custom,
  ascii,
  bigint,
  blob,
  boolean,
  counter,
  decimal,
  double,
  float,
  int,
  timestamp,
  uuid,
  varchar,
  varint,
  timeuuid,
  inet,
  date,
  time,
  smallint,
  tinyint,
  list,
  map,
  set,
  udt,
  tuple,
}

const _dataClassMap = const <int, DataClass>{
  0x0000: DataClass.custom,
  0x0001: DataClass.ascii,
  0x0002: DataClass.bigint,
  0x0003: DataClass.blob,
  0x0004: DataClass.boolean,
  0x0005: DataClass.counter,
  0x0006: DataClass.decimal,
  0x0007: DataClass.double,
  0x0008: DataClass.float,
  0x0009: DataClass.int,
  0x000B: DataClass.timestamp,
  0x000C: DataClass.uuid,
  0x000D: DataClass.varchar,
  0x000E: DataClass.varint,
  0x000F: DataClass.timeuuid,
  0x0010: DataClass.inet,
  0x0011: DataClass.date,
  0x0012: DataClass.time,
  0x0013: DataClass.smallint,
  0x0014: DataClass.tinyint,
  0x0020: DataClass.list,
  0x0021: DataClass.map,
  0x0022: DataClass.set,
  0x0030: DataClass.udt,
  0x0031: DataClass.tuple,
};

class DataType {
  final DataClass dataClass;

  /// String description of custom type
  final String customTypeName;

  /// Generic type parameters:
  /// List, Set: one value
  /// Map: two values: key, value
  /// Tuples: N values
  final List<DataType> parameters;

  DataType._(this.dataClass, this.customTypeName, this.parameters);

  const DataType.core(this.dataClass)
      : customTypeName = null,
        parameters = null;
}

DataType _parseValueType(BodyReader br) {
  final typeCode = br.parseShort();
  final dataClass = _dataClassMap[typeCode];
  if (dataClass == null) {
    throw new UnimplementedError('Unknown type code: $typeCode');
  }
  switch (dataClass) {
    case DataClass.custom:
      final customType = br.parseShortString();
      return new DataType._(DataClass.custom, customType, null);
    case DataClass.ascii:
    case DataClass.bigint:
    case DataClass.blob:
    case DataClass.boolean:
    case DataClass.counter:
    case DataClass.decimal:
    case DataClass.double:
    case DataClass.float:
    case DataClass.int:
    case DataClass.timestamp:
    case DataClass.uuid:
    case DataClass.varchar:
    case DataClass.varint:
    case DataClass.timeuuid:
    case DataClass.inet:
    case DataClass.date:
    case DataClass.time:
    case DataClass.smallint:
    case DataClass.tinyint:
      return new DataType.core(dataClass);
    default:
      throw new UnimplementedError('Unhandled data class: $dataClass');
  }
}

class Column {
  final String keyspace;
  final String table;
  final String column;
  final DataType dataType;

  Column(this.keyspace, this.table, this.column, this.dataType);
}

abstract class Row {
  List<Column> get columns;
  List get values;
  Map<String, dynamic> asMap();
}

class _Row implements Row {
  final List<Column> columns;
  final List values;

  _Row(this.columns, this.values);

  Map<String, dynamic> asMap() {
    final map = <String, dynamic>{};
    for (int i = 0; i < columns.length; i++) {
      map[columns[i].column] = values[i];
    }
    return map;
  }
}

abstract class RowsPage {
  List<Column> get columns;
  List<Row> get rows;
  bool get isLastPage;
  Future<RowsPage> nextPage();
}

class _RowsPage implements RowsPage {
  @override
  final List<Column> columns;
  @override
  final List<Row> rows;
  @override
  final bool isLastPage;

  _RowsPage(this.columns, this.rows, this.isLastPage);

  @override
  Future<RowsPage> nextPage() {
    throw new UnimplementedError();
  }
}
