import 'dart:typed_data';

import 'package:test/test.dart';

import 'package:cassandart/src/cassandart_impl.dart';

void main() {
  group('Core types', () {
    void encodeDecodeCore(RawType rawType, value, int length) {
      final encoded = encodeData(value);
      expect(encoded.length, length);
      final decoded = decodeData(new Type(rawType), encoded);
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

    test('double', () {
      encodeDecodeCore(RawType.double, 0.5, 8);
      encodeDecodeCore(RawType.double, -125.5, 8);
    });

    test('boolean', () {
      encodeDecodeCore(RawType.boolean, true, 1);
      encodeDecodeCore(RawType.boolean, false, 1);
    });

    test('blob', () {
      encodeDecodeCore(RawType.blob, new Uint8List.fromList([0, 2, 5]), 3);
      encodeDecodeCore(
          RawType.blob, new Uint8List.fromList([1, 255, 255, 9]), 4);
    });
  });
}
