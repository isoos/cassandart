part of 'cassandart_impl.dart';

Uint8List buildQuery({
  required String query,
  required Consistency? consistency,
  required values,
  required int? pageSize,
  required Uint8List? pagingState,
}) {
  consistency ??= Consistency.quorum;
  final hasPageSize = pageSize != null && pageSize > 0;

  final bw = _BodyWriter();
  bw.writeLongString(query);
  bw.writeShort(consistencyValue(consistency));

  int flag = 0x00;
  if (values != null) {
    flag = flag | 0x01;
    if (values is Map) {
      flag = flag | 0x40;
    }
  }
  if (hasPageSize) {
    flag = flag | 0x04;
  }
  if (pagingState != null) {
    flag = flag | 0x08;
  }
  bw.writeByte(flag);

  if (values != null && values is List) {
    bw.writeShort(values.length);

    for (final v in values) {
      if (v == null) {
        bw.writeNormalInt(-1);
      } else {
        bw.writeBytes(encodeData(v));
      }
    }
  } else if (values != null && values is Map) {
    bw.writeShort(values.length);
    values.forEach((k, v) {
      bw.writeShortString(k as String);
      if (v == null) {
        bw.writeNormalInt(-1);
      } else {
        bw.writeBytes(encodeData(v));
      }
    });
  } else if (values != null) {
    throw StateError('Unknown values: $values');
  }
  if (hasPageSize) {
    bw.writeNormalInt(pageSize!);
  }
  if (pagingState != null) {
    bw.writeBytes(pagingState);
  }

  return bw.toBytes();
}

int consistencyValue(Consistency value) {
  switch (value) {
    case Consistency.any:
      return 0x0000;
    case Consistency.one:
      return 0x0001;
    case Consistency.two:
      return 0x0002;
    case Consistency.three:
      return 0x0003;
    case Consistency.quorum:
      return 0x0004;
    case Consistency.all:
      return 0x0005;
    case Consistency.localQuorum:
      return 0x0006;
    case Consistency.eachQuorum:
      return 0x0007;
    case Consistency.serial:
      return 0x0008;
    case Consistency.localSerial:
      return 0x0009;
    case Consistency.localOne:
      return 0x000A;
    default:
      throw UnimplementedError('Unknown enum value: $value');
  }
}
