import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';

import 'package:cassandart/src/murmur3_hash.dart';

void main() {
  group('A group of tests', () {
    test('Murmur3 hashing test', () {
//      expect(_rshift(-1, 15), 562949953421311);
//      expect(_fmix64(14), -5035020264353794276);
      expect(murmur3Hash(','), 860700918917465446);
      expect(murmur3Hash('12_character'), -7110389279630717460);
      expect(murmur3Hash('longer_than_16_characters'), -1457872022925414645);
      expect(murmur3Hash('Roger'), -1289414324824907452);
      expect(murmur3Hash('Neon'), -1824832692919016840);

      expect(
          murmur3HashU8L(Uint8List.fromList([]
            ..addAll(Uint8List.fromList([0x00, 0x05]))
            ..addAll(utf8.encode('Roger'))
            ..addAll(Uint8List.fromList([0]))
            ..addAll(Uint8List.fromList([0x00, 0x04]))
            ..addAll(utf8.encode('Neon'))
            ..addAll(Uint8List.fromList([0])))),
          7199240612451099039);
      expect(murmur3Hash(['Roger', 'Neon']), 7199240612451099039);
    });
  });
}
