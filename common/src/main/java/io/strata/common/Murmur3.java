package io.strata.common;

/**
 * 32-bit MurmurHash3 (x86 variant), written from the public algorithm description. Used to map keys
 * to partitions: it is fast, well distributed and stable across JVMs, unlike {@code
 * Arrays.hashCode}, whose distribution is poor for similar keys.
 */
public final class Murmur3 {

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  private Murmur3() {}

  public static int hash32(byte[] data, int seed) {
    int h = seed;
    int len = data.length;
    int blocks = len / 4;
    for (int i = 0; i < blocks; i++) {
      int off = i * 4;
      int k =
          (data[off] & 0xff)
              | (data[off + 1] & 0xff) << 8
              | (data[off + 2] & 0xff) << 16
              | (data[off + 3] & 0xff) << 24;
      h ^= mixK(k);
      h = Integer.rotateLeft(h, 13) * 5 + 0xe6546b64;
    }
    int tail = blocks * 4;
    int k = 0;
    switch (len & 3) {
      case 3:
        k ^= (data[tail + 2] & 0xff) << 16;
        // fall through
      case 2:
        k ^= (data[tail + 1] & 0xff) << 8;
        // fall through
      case 1:
        k ^= data[tail] & 0xff;
        h ^= mixK(k);
        // fall through
      default:
        break;
    }
    h ^= len;
    return fmix(h);
  }

  public static long hash64(long a, long b) {
    // Cheap 64-bit mixer (SplitMix64 finaliser) for combining two longs, e.g. (key index, seed).
    long z = a * 0x9E3779B97F4A7C15L + b;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  private static int mixK(int k) {
    k *= C1;
    k = Integer.rotateLeft(k, 15);
    return k * C2;
  }

  private static int fmix(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
