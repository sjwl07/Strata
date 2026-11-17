package io.strata.common;

import com.google.protobuf.ByteString;

/**
 * Maps a key to a partition. The push job, the storage nodes and the client must all use this same
 * function, otherwise a key is written to one partition and looked up in another.
 */
public final class Partitioner {

  private static final int SEED = 0x5747A7A; // fixed forever: changing it re-shuffles every key

  private Partitioner() {}

  public static int partition(byte[] key, int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be > 0");
    }
    return Math.floorMod(Murmur3.hash32(key, SEED), numPartitions);
  }

  public static int partition(ByteString key, int numPartitions) {
    return partition(key.toByteArray(), numPartitions);
  }
}
