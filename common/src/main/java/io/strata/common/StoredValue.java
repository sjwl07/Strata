package io.strata.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * What the storage engine keeps for each key: the value plus the metadata needed to merge batch and
 * stream data correctly.
 *
 * <p>On-disk layout (big-endian): {@code [flags:1][ts:8][sourceId:4][value:...]}. Flag bit 0 marks
 * a tombstone (a delete that must be remembered so an older update can't resurrect the key).
 *
 * <p><b>Ordering</b> ({@link #compareTo}) is the merge rule (last-writer-wins): higher {@code ts}
 * wins; ties go to the higher {@code sourceId}, then a tombstone beats a live value, then the
 * lexicographically larger value. Because the order is total and deterministic, every replica that
 * sees the same set of updates ends with the same value, in any delivery order.
 */
public record StoredValue(long ts, int sourceId, boolean tombstone, byte[] value)
    implements Comparable<StoredValue> {

  /** Source id used for batch records: wins ties against stream records at the same ts. */
  public static final int BATCH_SOURCE = Integer.MAX_VALUE;

  private static final int HEADER = 13;
  private static final byte TOMBSTONE = 1;

  public StoredValue {
    value = value == null ? new byte[0] : value;
  }

  public static StoredValue live(long ts, int sourceId, byte[] value) {
    return new StoredValue(ts, sourceId, false, value);
  }

  public static StoredValue tombstone(long ts, int sourceId) {
    return new StoredValue(ts, sourceId, true, new byte[0]);
  }

  public byte[] encode() {
    ByteBuffer buf = ByteBuffer.allocate(HEADER + value.length);
    buf.put(tombstone ? TOMBSTONE : 0).putLong(ts).putInt(sourceId).put(value);
    return buf.array();
  }

  public static StoredValue decode(byte[] bytes) {
    if (bytes.length < HEADER) {
      throw new IllegalArgumentException("stored value too short: " + bytes.length);
    }
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    boolean tomb = (buf.get() & TOMBSTONE) != 0;
    long ts = buf.getLong();
    int source = buf.getInt();
    return new StoredValue(ts, source, tomb, Arrays.copyOfRange(bytes, HEADER, bytes.length));
  }

  /** Reads only the timestamp/source header, for the hot merge path. */
  public static long tsOf(byte[] encoded) {
    return ByteBuffer.wrap(encoded, 1, 8).getLong();
  }

  @Override
  public int compareTo(StoredValue o) {
    int c = Long.compare(ts, o.ts);
    if (c != 0) {
      return c;
    }
    c = Integer.compare(sourceId, o.sourceId);
    if (c != 0) {
      return c;
    }
    c = Boolean.compare(tombstone, o.tombstone);
    if (c != 0) {
      return c;
    }
    return Arrays.compareUnsigned(value, o.value);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof StoredValue s && compareTo(s) == 0;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(ts) * 31 + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return (tombstone ? "DEL" : "PUT")
        + "@"
        + ts
        + "/"
        + sourceId
        + (tombstone ? "" : "=" + value.length + "B");
  }
}
