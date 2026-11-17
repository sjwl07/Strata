package io.strata.common;

import io.strata.proto.v1.UpdateEvent;
import java.time.Duration;
import java.util.List;

/**
 * The stream of nearline updates for one store: a partitioned, append-only log addressed by
 * offsets, i.e. the subset of Kafka that Strata relies on. Partition {@code p} of the log feeds
 * partition {@code p} of the store, and every replica of that partition consumes it independently
 * (the log <i>is</i> the replication stream).
 */
public interface UpdateLog extends AutoCloseable {

  int numPartitions();

  /** Appends to the partition chosen by {@link Partitioner}. Returns the offset. */
  default long append(UpdateEvent event) {
    return append(Partitioner.partition(event.getKey(), numPartitions()), event);
  }

  long append(int partition, UpdateEvent event);

  /**
   * Up to {@code max} records starting at {@code fromOffset}; waits up to {@code timeout} if none
   * are available yet. Returns an empty list on timeout.
   *
   * <p><b>An empty result does not mean the log has no more records</b>: Kafka returns empty while
   * a fetch is in flight. Callers that need everything up to an offset must keep reading until they
   * reach it (see {@code PartitionReplica.replay}).
   */
  List<LogRecord> read(int partition, long fromOffset, int max, Duration timeout)
      throws InterruptedException;

  /** Offset the next appended record will get. */
  long endOffset(int partition);

  /**
   * First offset whose append time is {@code >= appendTimeMs}, or {@link #endOffset} if none
   * (Kafka's {@code offsetsForTimes}).
   */
  long offsetForTime(int partition, long appendTimeMs);

  /** A record with its position and the broker-assigned append time. */
  record LogRecord(long offset, long appendTimeMs, UpdateEvent event) {}

  @Override
  default void close() {}
}
