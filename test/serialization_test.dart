import 'dart:typed_data';

import 'package:test/test.dart';

import 'package:cassandart/src/cassandart_impl.dart';

void main() {
  group('Core types', () {
    void encodeDecodeCore(DataClass dataClass, value, int length) {
      final encoded = encodeData(value);
      expect(encoded.length, length);
      final decoded = decodeData(new DataType.core(dataClass), encoded);
      expect(decoded, value);
    }

    test('string', () {
      encodeDecodeCore(DataClass.ascii, 'abc123', 6);
      encodeDecodeCore(DataClass.varchar, 'Ábc123', 7);
    });

    test('bigint', () {
      encodeDecodeCore(DataClass.bigint, 123, 8);
      encodeDecodeCore(DataClass.bigint, -2349257347856, 8);
    });

    test('double', () {
      encodeDecodeCore(DataClass.double, 0.5, 8);
      encodeDecodeCore(DataClass.double, -125.5, 8);
    });

    test('boolean', () {
      encodeDecodeCore(DataClass.boolean, true, 1);
      encodeDecodeCore(DataClass.boolean, false, 1);
    });

    test('blob', () {
      encodeDecodeCore(DataClass.blob, new Uint8List.fromList([0, 2, 5]), 3);
      encodeDecodeCore(
          DataClass.blob, new Uint8List.fromList([1, 255, 255, 9]), 4);
    });
  });
}
