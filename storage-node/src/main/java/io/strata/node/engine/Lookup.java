package io.strata.node.engine;

/** Result of a point read. {@code value} is null when not found. */
public record Lookup(boolean found, byte[] value, long version) {
  public static Lookup missing(long version) {
    return new Lookup(false, null, version);
  }
}
