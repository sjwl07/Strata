package io.strata.node.engine;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.Arrays;

/**
 * Result of a point read. To avoid copying on the hot path, the value is a slice of {@code raw}
 * starting at {@code offset} (the stored record minus its header). {@link #value()} copies; {@link
 * #valueBytes()} wraps the slice for protobuf without copying.
 */
public record Lookup(boolean found, byte[] raw, int offset, long version) {

  public static Lookup missing(long version) {
    return new Lookup(false, null, 0, version);
  }

  public static Lookup of(byte[] value, long version) {
    return new Lookup(true, value, 0, version);
  }

  /** A copy of the value, or null if not found. */
  public byte[] value() {
    return found ? Arrays.copyOfRange(raw, offset, raw.length) : null;
  }

  /** The value as a zero-copy ByteString (raw is never modified after the read). */
  public ByteString valueBytes() {
    return found
        ? UnsafeByteOperations.unsafeWrap(raw, offset, raw.length - offset)
        : ByteString.EMPTY;
  }
}
