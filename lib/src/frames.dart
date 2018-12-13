part of 'cassandart_impl.dart';

class FrameHeader {
  final bool isRequest;
  final int protocolVersion;
  final bool isCompressed;
  final bool requiresTracing;
  final bool hasCustomPayload;
  final bool hasWarning;
  final int streamId;
  final int opcode;
  final int length;

  FrameHeader({
    @required this.isRequest,
    @required this.protocolVersion,
    @required this.isCompressed,
    @required this.requiresTracing,
    @required this.hasCustomPayload,
    @required this.hasWarning,
    @required this.streamId,
    @required this.opcode,
    @required this.length,
  });

  bool get isResponse => !isRequest;

  Uint8List toHeaderBytes() {
    final list = new Uint8List(9);
    final data = new ByteData.view(list.buffer);
    final flag = (isCompressed ? _compressedMask : 0x00) |
        (requiresTracing ? _tracingMask : 0x00) |
        (hasCustomPayload ? _customPayloadMask : 0x00) |
        (hasWarning ? _warningMask : 0x00);
    data.setInt8(0, (isResponse ? _responseMask : 0x00) | protocolVersion);
    data.setInt8(1, flag);
    data.setInt16(2, streamId, Endian.big);
    data.setInt8(4, opcode);
    data.setInt32(5, length, Endian.big);
    return list;
  }
}

class Frame {
  final FrameHeader header;
  final Uint8List body;

  Frame(this.header, this.body);

  int get opcode => header.opcode;
  int get streamId => header.streamId;
}

Stream<Frame> parseFrames(Stream<List<int>> input) {
  return new _FrameStreamTransformer().parseFrames(input);
}

class FrameSink implements Sink<Frame> {
  final Sink<List<int>> _output;

  FrameSink(this._output);

  @override
  void add(Frame frame) {
    _output.add(frame.header.toHeaderBytes());
    if (frame.body != null && frame.body.isNotEmpty) {
      _output.add(frame.body);
    }
  }

  @override
  void close() {
    _output.close();
  }
}

class _FrameStreamTransformer {
  final _buffer = new ByteDataReader();
  FrameHeader _header;

  Stream<Frame> parseFrames(Stream<List<int>> input) {
    return input.transform(new StreamTransformer.fromHandlers(
      handleData: (List<int> data, EventSink<Frame> sink) {
        _buffer.add(data);
        for (; _emitFrame(sink);) {}
      },
    ));
  }

  bool _emitFrame(EventSink<Frame> sink) {
    if (_header == null && _buffer.remainingLength < 9) {
      return false;
    }
    if (_header == null) {
      final headerBytes = _buffer.read(9);
      final version = headerBytes[0];
      final isResponse = (version & _responseMask) == _responseMask;
      final protocolVersion = version & _protocolVersionMask;
      final flags = headerBytes[1];
      final isCompressed = (flags & _compressedMask) == _compressedMask;
      final requiresTracing = (flags & _tracingMask) == _tracingMask;
      final hasCustomPayload =
          (flags & _customPayloadMask) == _customPayloadMask;
      final hasWarning = (flags & _warningMask) == _warningMask;
      final streamId = (headerBytes[2] << 8) + headerBytes[3];
      final opcode = headerBytes[4];
      final length = (headerBytes[5] << 24) +
          (headerBytes[6] << 16) +
          (headerBytes[7] << 8) +
          headerBytes[8];

      _header = new FrameHeader(
        isRequest: !isResponse,
        protocolVersion: protocolVersion,
        isCompressed: isCompressed,
        requiresTracing: requiresTracing,
        hasCustomPayload: hasCustomPayload,
        hasWarning: hasWarning,
        streamId: streamId,
        opcode: opcode,
        length: length,
      );
    }

    if (_header != null && _buffer.remainingLength < _header.length) {
      return false;
    }

    final Uint8List body =
        _header.length == 0 ? null : _buffer.read(_header.length);
    final frame = new Frame(_header, body);
    _header = null;
    sink.add(frame);
    return true;
  }
}

const _responseMask = 0x80;
const _protocolVersionMask = 0x7f;
const _compressedMask = 0x01;
const _tracingMask = 0x02;
const _customPayloadMask = 0x04;
const _warningMask = 0x08;
