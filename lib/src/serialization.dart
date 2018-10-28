part of 'cassandart_impl.dart';

decodeData(DataType type, List<int> data) {
  switch (type.dataClass) {
    case DataClass.ascii:
      return ascii.decode(data);
    case DataClass.varchar:
      return utf8.decode(data);
    default:
      throw new UnimplementedError('Decode of ${type.dataClass} not implemented.');
  }
}

List<int> encodeData(value) {
  if (value is String) {
    return utf8.encode(value);
  }
  throw new UnimplementedError('Encode of $value not implemented.');
}

class BodyWriter extends CombinedListView<int> {
  final _chunks;

  BodyWriter._(List<List<int>> chunks)
      : _chunks = chunks,
        super(chunks);

  factory BodyWriter() => new BodyWriter._(<List<int>>[]);

  void writeByte(int value) {
    _chunks.add([value]);
  }

  void writeBytes(List<int> value) {
    writeInt(value.length);
    _chunks.add(value);
  }

  void writeShort(int value) {
    final data = new ByteData(2);
    data.setInt16(0, value, Endian.big);
    _chunks.add(new Uint8List.view(data.buffer));
  }

  void writeInt(int value) {
    final data = new ByteData(4);
    data.setInt32(0, value, Endian.big);
    _chunks.add(new Uint8List.view(data.buffer));
  }

  void writeString(String value) {
    final data = utf8.encode(value);
    writeShort(data.length);
    _chunks.add(data);
  }

  void writeLongString(String value) {
    final data = utf8.encode(value);
    writeInt(data.length);
    _chunks.add(data);
  }

  void writeStringMap(Map<String, String> map) {
    writeShort(map.length);
    map.forEach((k, v) {
      writeString(k);
      writeString(v);
    });
  }
}

class BodyReader {
  final List<int> _body;
  int _offset = 0;
  BodyReader(this._body);

  List<int> readBytes() {
    final length = readInt();
    final list = _body.sublist(_offset, _offset + length);
    _offset += length;
    return list;
  }

  int readShort() {
    return (_body[_offset++] << 8) + _body[_offset++];
  }

  int readInt() {
    return (_body[_offset++] << 24) +
        (_body[_offset++] << 16) +
        (_body[_offset++] << 8) +
        (_body[_offset++]);
  }

  String readString() {
    final length = readShort();
    final str = utf8
        .decode(new LimitListView(new OffsetListView(_body, _offset), length));
    _offset += length;
    return str;
  }
}
