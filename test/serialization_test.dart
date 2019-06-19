import 'dart:typed_data';

import 'package:test/test.dart';

import 'package:cassandart/src/cassandart_impl.dart';

void main() {
  group('Core types', () {
    void encodeDecodeCore(RawType rawType, value, int length) {
      final encoded = encodeData(value);
      expect(encoded.length, length);
      final decoded = decodeData(Type(rawType), encoded);
      expect(decoded, value);
    }

    test('string', () {
      encodeDecodeCore(RawType.ascii, 'abc123', 6);
      encodeDecodeCore(RawType.varchar, 'Ábc123', 7);
    });

    test('bigint', () {
      encodeDecodeCore(RawType.bigint, 123, 8);
      encodeDecodeCore(RawType.bigint, -2349257347856, 8);
    });

    test('timestamp', () {
      encodeDecodeCore(RawType.timestamp, 123, 8);
      encodeDecodeCore(RawType.timestamp, 2349257347856, 8);
    });

    test('counter', () {
      encodeDecodeCore(RawType.counter, 123, 8);
      encodeDecodeCore(RawType.counter, 2349257347856, 8);
    });

    test('double', () {
      encodeDecodeCore(RawType.double, 0.5, 8);
      encodeDecodeCore(RawType.double, -125.5, 8);
    });

    test('boolean', () {
      encodeDecodeCore(RawType.boolean, true, 1);
      encodeDecodeCore(RawType.boolean, false, 1);
    });

    test('blob', () {
      encodeDecodeCore(RawType.blob, Uint8List.fromList([0, 2, 5]), 3);
      encodeDecodeCore(RawType.blob, Uint8List.fromList([1, 255, 255, 9]), 4);
    });

    test('timeuuid', () {
      encodeDecodeCore(
          RawType.timeuuid,
          [
            19,
            129,
            64,
            0,
            29,
            210,
            17,
            178,
            128,
            128,
            128,
            128,
            128,
            128,
            128,
            128,
          ],
          16);
      encodeDecodeCore(
          RawType.timeuuid,
          [
            101,
            174,
            107,
            96,
            146,
            187,
            17,
            233,
            241,
            172,
            125,
            25,
            111,
            185,
            209,
            159,
          ],
          16);
    });
  });
}
