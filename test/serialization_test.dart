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

    test('timestamp', () {
      encodeDecodeCore(DataClass.timestamp, 123, 8);
      encodeDecodeCore(DataClass.timestamp, 2349257347856, 8);
    });

    test('counter', () {
      encodeDecodeCore(DataClass.counter, 123, 8);
      encodeDecodeCore(DataClass.counter, 2349257347856, 8);
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

    test('timeuuid', () {
      encodeDecodeCore(DataClass.timeuuid, [19, 129, 64, 0, 29, 210, 17, 178,
        128, 128, 128, 128, 128, 128, 128, 128], 16);
      encodeDecodeCore(DataClass.timeuuid, [101, 174, 107, 96, 146, 187, 17,
        233, 241, 172, 125, 25, 111, 185, 209, 159], 16);
    });
  });
}
