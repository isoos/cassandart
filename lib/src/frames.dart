part of 'cassandart_impl.dart';

class Frame {
  final bool isRequest;
  final int protocolVersion;
  final bool isCompressed;
  final bool requiresTracing;
  final bool hasCustomPayload;
  final bool hasWarning;
  final int streamId;
  final int opcode;
  final Uint8List body;

  Frame({
    @required this.isRequest,
    @required this.protocolVersion,
    @required this.isCompressed,
    @required this.requiresTracing,
    @required this.hasCustomPayload,
    @required this.hasWarning,
    @required this.streamId,
    @required this.opcode,
    @required this.body,
  });

  bool get isResponse => !isRequest;
  int get length => body == null ? 0 : body.length;

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

Stream<Frame> parseFrames(Stream<List<int>> input) {
  return new _FrameStreamTransformer().parseFrames(input);
}

class FrameSink implements Sink<Frame> {
  final Sink<List<int>> _output;

  FrameSink(this._output);

  @override
  void add(Frame frame) {
    _output.add(frame.toHeaderBytes());
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
  final _queue = new DoubleLinkedQueue<Uint8List>();

  Stream<Frame> parseFrames(Stream<List<int>> input) {
    return input.transform(new StreamTransformer.fromHandlers(
      handleData: (List<int> data, EventSink<Frame> sink) {
        Uint8List bytes;
        if (data is Uint8List) {
          bytes = data;
        } else {
          bytes = new Uint8List.fromList(data);
        }
        _queue.add(bytes);
        for (; _emitFrame(sink);) {}
      },
    ));
  }

  bool _emitFrame(EventSink<Frame> sink) {
    final combined = new CombinedListView(_queue.toList());
    if (combined.length < 9) {
      return false;
    }
    final version = combined[0];
    final isResponse = (version & _responseMask) == _responseMask;
    final protocolVersion = version & _protocolVersionMask;
    final flags = combined[1];
    final isCompressed = (flags & _compressedMask) == _compressedMask;
    final requiresTracing = (flags & _tracingMask) == _tracingMask;
    final hasCustomPayload = (flags & _customPayloadMask) == _customPayloadMask;
    final hasWarning = (flags & _warningMask) == _warningMask;
    final streamId = (combined[2] << 8) + combined[3];
    final opcode = combined[4];
    final length = (combined[5] << 24) +
        (combined[6] << 16) +
        (combined[7] << 8) +
        combined[8];

    final totalLength = length + 9;
    if (combined.length < totalLength) {
      return false;
    }

    final body = new Uint8List(length);
    body.setRange(0, length, combined, 9);

    int removeLength = totalLength;
    while (removeLength > 0) {
      final next = _queue.removeFirst();
      if (next.length <= removeLength) {
        removeLength -= next.length;
        continue;
      }
      final buffer = new Uint8List(next.length - removeLength);
      buffer.setRange(0, buffer.length, next, removeLength);
      _queue.addFirst(buffer);
    }

    sink.add(new Frame(
      isRequest: !isResponse,
      protocolVersion: protocolVersion,
      isCompressed: isCompressed,
      requiresTracing: requiresTracing,
      hasCustomPayload: hasCustomPayload,
      hasWarning: hasWarning,
      streamId: streamId,
      opcode: opcode,
      body: body,
    ));
    return true;
  }
}

const _responseMask = 0x80;
const _protocolVersionMask = 0x7f;
const _compressedMask = 0x01;
const _tracingMask = 0x02;
const _customPayloadMask = 0x04;
const _warningMask = 0x08;
