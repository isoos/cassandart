part of 'cassandart_impl.dart';

ByteData _byteData(List<int> data) {
  return new ByteData.view(new Uint8List.fromList(data).buffer);
}

class TypedValue<T> {
  final DataType type;
  final T value;

  TypedValue._(this.type, this.value);

  static TypedValue<int> int8(int value) =>
      new TypedValue._(const DataType.core(DataClass.tinyint), value);

  static TypedValue<int> int16(int value) =>
      new TypedValue._(const DataType.core(DataClass.smallint), value);

  static TypedValue<int> int32(int value) =>
      new TypedValue._(const DataType.core(DataClass.int), value);

  static TypedValue<double> float(double value) =>
      new TypedValue._(const DataType.core(DataClass.float), value);
}

decodeData(DataType type, List<int> data) {
  switch (type.dataClass) {
    case DataClass.blob:
      return data;
    case DataClass.boolean:
      return data[0] != 0;
    case DataClass.ascii:
      return ascii.decode(data);
    case DataClass.varchar:
      return utf8.decode(data);
    case DataClass.bigint:
      return _byteData(data).getInt64(0, Endian.big);
    case DataClass.int:
      return _byteData(data).getInt32(0, Endian.big);
    case DataClass.smallint:
      return _byteData(data).getInt16(0, Endian.big);
    case DataClass.tinyint:
      return _byteData(data).getInt8(0);
    case DataClass.float:
      return _byteData(data).getFloat32(0, Endian.big);
    case DataClass.double:
      return _byteData(data).getFloat64(0, Endian.big);
    default:
      throw new UnimplementedError(
          'Decode of ${type.dataClass} not implemented.');
  }
}

Uint8List encodeString(String value) => castBytes(utf8.encode(value));

Uint8List encodeBigint(int value) {
  final data = new ByteData(8);
  data.setInt64(0, value, Endian.big);
  return new Uint8List.view(data.buffer);
}

Uint8List encodeDouble(double value) {
  final data = new ByteData(8);
  data.setFloat64(0, value, Endian.big);
  return new Uint8List.view(data.buffer);
}

final _boolFalse = new Uint8List.fromList([0]);
final _boolTrue = new Uint8List.fromList([1]);

Uint8List encodeData(value) {
  if (value is String) {
    return encodeString(value);
  } else if (value is int) {
    return encodeBigint(value);
  } else if (value is double) {
    return encodeDouble(value);
  } else if (value is bool) {
    return value ? _boolTrue : _boolFalse;
  } else if (value is Uint8List) {
    return value;
  } else if (value is TypedValue<int> &&
      value.type.dataClass == DataClass.tinyint) {
    return castBytes([value.value]);
  } else if (value is TypedValue<int> &&
      value.type.dataClass == DataClass.smallint) {
    final data = new ByteData(2);
    data.setInt16(0, value.value, Endian.big);
    return new Uint8List.view(data.buffer);
  } else if (value is TypedValue<int> &&
      value.type.dataClass == DataClass.int) {
    final data = new ByteData(4);
    data.setInt32(0, value.value, Endian.big);
    return new Uint8List.view(data.buffer);
  } else if (value is TypedValue<double> &&
      value.type.dataClass == DataClass.float) {
    final data = new ByteData(4);
    data.setFloat32(0, value.value, Endian.big);
    return new Uint8List.view(data.buffer);
  } else {
    throw new UnimplementedError('Encode of $value not implemented.');
  }
}

class _BodyWriter extends ByteDataWriter {
  void writeByte(int value) {
    write(new Uint8List(1)..[0] = value);
  }

  void writeBytes(Uint8List value) {
    writeNormalInt(value.length);
    write(value);
  }

  void writeShort(int value) {
    writeInt16(value);
  }

  void writeNormalInt(int value) {
    writeInt32(value);
  }

  void writeShortString(String value) {
    final data = utf8.encode(value);
    writeShort(data.length);
    write(data);
  }

  void writeLongString(String value) {
    final data = utf8.encode(value);
    writeNormalInt(data.length);
    write(data);
  }

  void writeStringMap(Map<String, String> map) {
    writeShort(map.length);
    map.forEach((k, v) {
      writeShortString(k);
      writeShortString(v);
    });
  }
}

class _BodyReader extends ByteDataReader {
  _BodyReader(Uint8List body) {
    add(body);
  }

  Uint8List parseBytes({bool copy: false}) {
    final length = parseInt();
    return read(length, copy: copy);
  }

  int parseShort() => readInt16();

  int parseInt() => readInt32();

  String parseShortString() {
    final length = parseShort();
    final buffer = read(length);
    return utf8.decode(buffer);
  }
}
