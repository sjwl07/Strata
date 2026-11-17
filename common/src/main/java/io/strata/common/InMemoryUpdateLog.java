package io.strata.common;

import io.strata.proto.v1.UpdateEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * In-process {@link UpdateLog} with Kafka semantics: per-partition total order, dense offsets from
 * 0, and append times that never go backwards within a partition (like Kafka's LogAppendTime). The
 * clock is injectable so tests and the correctness checker can control time exactly.
 */
public final class InMemoryUpdateLog implements UpdateLog {

  private final Part[] parts;
  private final LongSupplier clock;
  private final boolean emptyFirstRead;

  public InMemoryUpdateLog(int numPartitions) {
    this(numPartitions, System::currentTimeMillis);
  }

  public InMemoryUpdateLog(int numPartitions, LongSupplier clock) {
    this(numPartitions, clock, false);
  }

  /**
   * @param emptyFirstRead if true, the first read at any offset returns nothing even though records
   *     are available, and a retry returns them. That's what Kafka's {@code poll} does while a
   *     fetch is in flight. Tests use it to make sure no code treats an empty read as "end of log".
   */
  public InMemoryUpdateLog(int numPartitions, LongSupplier clock, boolean emptyFirstRead) {
    this.parts = new Part[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      parts[i] = new Part();
    }
    this.clock = clock;
    this.emptyFirstRead = emptyFirstRead;
  }

  private static final class Part {
    final ReentrantLock lock = new ReentrantLock();
    final Condition appended = lock.newCondition();
    final List<LogRecord> records = new ArrayList<>();
    long lastAppendTime = Long.MIN_VALUE;
    final java.util.Set<Long> fetched = new java.util.HashSet<>();
  }

  @Override
  public int numPartitions() {
    return parts.length;
  }

  @Override
  public long append(int partition, UpdateEvent event) {
    Part p = parts[partition];
    p.lock.lock();
    try {
      long t = Math.max(clock.getAsLong(), p.lastAppendTime);
      p.lastAppendTime = t;
      long offset = p.records.size();
      p.records.add(new LogRecord(offset, t, event));
      p.appended.signalAll();
      return offset;
    } finally {
      p.lock.unlock();
    }
  }

  @Override
  public List<LogRecord> read(int partition, long fromOffset, int max, Duration timeout)
      throws InterruptedException {
    Part p = parts[partition];
    if (emptyFirstRead) {
      boolean firstTime;
      p.lock.lock();
      try {
        firstTime = fromOffset < p.records.size() && p.fetched.add(fromOffset);
      } finally {
        p.lock.unlock();
      }
      if (firstTime) {
        return Collections.emptyList(); // "fetch in flight"
      }
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    p.lock.lock();
    try {
      while (p.records.size() <= fromOffset) {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
          return Collections.emptyList();
        }
        p.appended.await(left, TimeUnit.NANOSECONDS);
      }
      int from = (int) fromOffset;
      int to = (int) Math.min(p.records.size(), fromOffset + max);
      return List.copyOf(p.records.subList(from, to));
    } finally {
      p.lock.unlock();
    }
  }

  @Override
  public long endOffset(int partition) {
    Part p = parts[partition];
    p.lock.lock();
    try {
      return p.records.size();
    } finally {
      p.lock.unlock();
    }
  }

  @Override
  public long offsetForTime(int partition, long appendTimeMs) {
    Part p = parts[partition];
    p.lock.lock();
    try {
      // Append times are non-decreasing, so binary search works.
      int lo = 0;
      int hi = p.records.size();
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (p.records.get(mid).appendTimeMs() < appendTimeMs) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return lo;
    } finally {
      p.lock.unlock();
    }
  }
}
