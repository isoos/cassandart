import 'dart:convert';
import 'dart:typed_data';

int _rshift(int x, int r) {
  if(r <= 0) return x;
  if(x < 0) {
    x ^= 0x8000000000000000;
    x >>= 1;
    x ^= 0x4000000000000000;
    r -= 1;
  }
  return x >> r;
}

int _ROTL64(int x, int r) {
  return (x << r) | _rshift(x, 64 - r);
}

int _fmix64(int k) {
  k ^= _rshift(k, 33);
  k *= 0xff51afd7ed558ccd;
  k ^= _rshift(k, 33);
  k *= 0xc4ceb9fe1a85ec53;
  k ^= _rshift(k, 33);
  return k;
}

int murmur3_hash(String inputString, {int seed = 0}) {
  final input = Uint8List.fromList(utf8.encode(inputString));
  final input64View =Int64List.view(input.buffer);

  int h1 = seed;
  int h2 = seed;

  const c1 = 0x87c37b91114253d5;
  const c2 = 0x4cf5ad432745937f;

  // body

  for (int pos = 0; pos < input.length ~/ 16; pos++) {
    int k1 = input64View[2*pos];
    int k2 = input64View[2*pos + 1];

    k1 *= c1;
    k1 = _ROTL64(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = _ROTL64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2 = _ROTL64(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = _ROTL64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  // tail

  int k1 = 0;
  int k2 = 0;
  int pos = input.length ~/ 16;
  for (int i = 0; pos * 16 + i < input.length; ++i) {
    if (i == 8) break;
    k1 ^= input[pos * 16 + i] << (i * 8);
  }
  for (int i = 0; pos * 16 + 8 + i < input.length; ++i) {
    k2 ^= input[pos * 16 + 8 + i] << (i * 8);
  }

  k2 *= c2;
  k2 = _ROTL64(k2, 33);
  k2 *= c1;
  h2 ^= k2;

  k1 *= c1;
  k1 = _ROTL64(k1, 31);
  k1 *= c2;
  h1 ^= k1;

  // finalization

  h1 ^= input.length;
  h2 ^= input.length;

  h1 += h2;
  h2 += h1;

  h1 = _fmix64(h1);
  h2 = _fmix64(h2);

  h1 += h2;
  h2 += h1;

  return h1;
}
