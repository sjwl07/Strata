package io.strata.push;

import io.strata.common.Murmur3;
import java.nio.charset.StandardCharsets;
import java.util.SplittableRandom;

/**
 * Deterministic dataset family for experiments. Snapshot 0 holds keys {@code k000000000000 ..} with
 * random values. In each later snapshot every key independently changes with probability {@code
 * changeFraction}; a change is a delete with probability {@code deleteFraction}, otherwise a fresh
 * value (a deleted key may come back later). Any snapshot can be regenerated from the parameters
 * alone, so the push job can diff snapshot N against N-1 without storing either.
 */
public record SyntheticDataset(
    long numKeys,
    int valueSize,
    long seed,
    int snapshot,
    double changeFraction,
    double deleteFraction)
    implements Dataset {

  public static SyntheticDataset initial(long numKeys, int valueSize, long seed) {
    return new SyntheticDataset(numKeys, valueSize, seed, 0, 0, 0);
  }

  public SyntheticDataset next(double changeFraction, double deleteFraction) {
    return new SyntheticDataset(
        numKeys, valueSize, seed, snapshot + 1, changeFraction, deleteFraction);
  }

  public static byte[] key(long i) {
    return "k%012d".formatted(i).getBytes(StandardCharsets.US_ASCII);
  }

  private double uniform(long i, int s, long salt) {
    return (Murmur3.hash64(i, seed * 1_000_003L + s * 7919L + salt) >>> 11) * 0x1.0p-53;
  }

  boolean changed(long i, int s) {
    return s == 0 || uniform(i, s, 1) < changeFraction;
  }

  boolean deleted(long i, int s) {
    return s > 0 && uniform(i, s, 2) < deleteFraction;
  }

  /** The value of key {@code i} in this snapshot, or null if it doesn't exist. */
  public byte[] valueOf(long i) {
    int s = snapshot;
    while (!changed(i, s)) {
      s--;
    }
    if (deleted(i, s)) {
      return null;
    }
    byte[] v = new byte[valueSize];
    new SplittableRandom(Murmur3.hash64(i, seed ^ ((long) s << 32))).nextBytes(v);
    return v;
  }

  @Override
  public void forEachInPartition(int partition, int numPartitions, Visitor visitor)
      throws java.io.IOException {
    for (long i = 0; i < numKeys; i++) {
      byte[] k = key(i);
      if (io.strata.common.Partitioner.partition(k, numPartitions) == partition) {
        byte[] v = valueOf(i); // only generate values for rows we keep
        if (v != null) {
          visitor.visit(k, v);
        }
      }
    }
  }

  @Override
  public void forEach(Visitor visitor) throws java.io.IOException {
    for (long i = 0; i < numKeys; i++) {
      byte[] v = valueOf(i);
      if (v != null) {
        visitor.visit(key(i), v);
      }
    }
  }
}
