import 'package:test/test.dart';

import 'package:cassandart/src/murmur3_hash.dart';

void main() {
  group('A group of tests', () {
    test('Murmur3 hashing test', () {
//      expect(_rshift(-1, 15), 562949953421311);
//      expect(_fmix64(14), -5035020264353794276);
      expect(murmur3_hash(','), 860700918917465446);
      expect(murmur3_hash('12_character'), -7110389279630717460);
      expect(murmur3_hash('longer_than_16_characters'), -1457872022925414645);
    });
  });
}
