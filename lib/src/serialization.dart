part of 'cassandart_impl.dart';

ByteData _byteData(List<int> data) {
  return ByteData.view(Uint8List.fromList(data).buffer);
}

class Value<T> {
  final Type type;
  final T value;

  Value._(this.type, this.value);

  static Value<int> int8(int value) =>
      Value._(const Type(RawType.tinyint), value);

  static Value<int> int16(int value) =>
      Value._(const Type(RawType.smallint), value);

  static Value<int> int32(int value) => Value._(const Type(RawType.int), value);

  static Value<double> float(double value) =>
      Value._(const Type(RawType.float), value);
}

decodeData(Type type, List<int> data) {
  switch (type.rawType) {
    case RawType.blob:
      return data;
    case RawType.boolean:
      return data[0] != 0;
    case RawType.ascii:
      return ascii.decode(data);
    case RawType.varchar:
      return utf8.decode(data);
    case RawType.bigint:
    case RawType.counter:
    case RawType.timestamp:
      return _byteData(data).getInt64(0, Endian.big);
    case RawType.int:
      return _byteData(data).getInt32(0, Endian.big);
    case RawType.smallint:
      return _byteData(data).getInt16(0, Endian.big);
    case RawType.tinyint:
      return _byteData(data).getInt8(0);
    case RawType.float:
      return _byteData(data).getFloat32(0, Endian.big);
    case RawType.double:
      return _byteData(data).getFloat64(0, Endian.big);
    case RawType.timeuuid:
      return Uint8List.fromList(data);
    case RawType.inet:
      return decodeInet(data);
    default:
      throw UnimplementedError('Decode of ${type.rawType} not implemented.');
  }
}

InternetAddress decodeInet(List<int> data) {
  if(data.length == 4) { // IPv4
    final address = data.join('.');
    return InternetAddress(address);
  }
  else if(data.length == 16) { // IPv6
    String address = '';
    for(int i = 0; i < 16; i += 2) {
      address += ((data[i] << 8) + data[i+1]).toRadixString(16);
      if(i < 14) {
        address += ':';
      }
    }
    return InternetAddress(address);
  } else {
    throw ArgumentError('Length of data must be 4 or 16 for inet address.');
  }
}

Uint8List encodeString(String value) => castBytes(utf8.encode(value));

Uint8List encodeBigint(int value) {
  final data = ByteData(8);
  data.setInt64(0, value, Endian.big);
  return Uint8List.view(data.buffer);
}

Uint8List encodeDouble(double value) {
  final data = ByteData(8);
  data.setFloat64(0, value, Endian.big);
  return Uint8List.view(data.buffer);
}

final _boolFalse = Uint8List.fromList([0]);
final _boolTrue = Uint8List.fromList([1]);

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
  } else if (value is List<int>) {
    return Uint8List.fromList(value);
  } else if (value is Value<int> && value.type.rawType == RawType.tinyint) {
    return castBytes([value.value]);
  } else if (value is Value<int> && value.type.rawType == RawType.smallint) {
    final data = ByteData(2);
    data.setInt16(0, value.value, Endian.big);
    return Uint8List.view(data.buffer);
  } else if (value is Value<int> && value.type.rawType == RawType.int) {
    final data = ByteData(4);
    data.setInt32(0, value.value, Endian.big);
    return Uint8List.view(data.buffer);
  } else if (value is Value<double> && value.type.rawType == RawType.float) {
    final data = ByteData(4);
    data.setFloat32(0, value.value, Endian.big);
    return Uint8List.view(data.buffer);
  } else if(value is InternetAddress) {
    return value.rawAddress;
  } else {
    throw UnimplementedError('Encode of $value not implemented. '
        'Type: ${value.runtimeType}');
  }
}

class _BodyWriter extends ByteDataWriter {
  void writeByte(int value) {
    write(Uint8List(1)..[0] = value);
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

  Uint8List parseBytes({bool copy = false}) {
    final length = parseInt();
    return length == -1 ? null : read(length);
  }

  int parseShort() => readInt16();

  int parseInt() => readInt32();

  String parseShortString() {
    final length = parseShort();
    final buffer = read(length);
    return utf8.decode(buffer);
  }
}
