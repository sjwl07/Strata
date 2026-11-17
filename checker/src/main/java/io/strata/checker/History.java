package io.strata.checker;

import io.strata.common.StoredValue;
import java.util.List;

/**
 * A generated scenario, in wall-clock order.
 *
 * <p>Two clocks matter. An update's <b>event time</b> ({@code ts}) is when it happened at the
 * source. Its <b>append time</b> is when it reached the log. Normally they're close, but an update
 * can be <i>late</i> (appended long after it happened) or <i>future-dated</i> (the producer's clock
 * is ahead, so ts > append time, bounded by the skew).
 */
public record History(long seed, int numKeys, List<Action> actions) {

  /** Something that happens at wall-clock time {@code at}. */
  public sealed interface Action permits Append, Push, Swap, Check, Restart {
    long at();
  }

  /**
   * An update reaches the log. {@code duplicate} marks a re-delivery of an earlier event (producer
   * retry); its {@code event} is identical to the original's.
   */
  public record Append(long at, Event event, boolean duplicate) implements Action {}

  /** Push job publishes a snapshot taken at cutoff {@code watermark}, and nodes prepare it. */
  public record Push(long at, long version, long watermark) implements Action {}

  /** Nodes switch to {@code version} (some time after it was prepared). */
  public record Swap(long at, long version) implements Action {}

  /** Read every key from every replica and compare with the oracle. */
  public record Check(long at) implements Action {}

  /** Replica B crashes and comes back empty (bootstraps from the latest version + log). */
  public record Restart(long at) implements Action {}

  /** One update. Values are unique per event ("e{id}"), so a read tells us which event it saw. */
  public record Event(int id, int key, long ts, int sourceId, boolean delete) {
    public byte[] valueBytes() {
      return ("e" + id).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    public io.strata.proto.v1.UpdateEvent toProto(byte[] keyBytes) {
      io.strata.proto.v1.UpdateEvent.Builder b =
          io.strata.proto.v1.UpdateEvent.newBuilder()
              .setKey(com.google.protobuf.ByteString.copyFrom(keyBytes))
              .setTs(ts)
              .setSourceId(sourceId)
              .setDelete(delete);
      if (!delete) {
        b.setValue(com.google.protobuf.ByteString.copyFrom(valueBytes()));
      }
      return b.build();
    }

    public StoredValue stored() {
      return delete
          ? StoredValue.tombstone(ts, sourceId)
          : StoredValue.live(ts, sourceId, valueBytes());
    }
  }
}
