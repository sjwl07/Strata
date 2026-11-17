package io.strata.node.engine;

import io.strata.common.StoredValue;
import io.strata.common.UpdateLog;
import io.strata.common.UpdateLog.LogRecord;
import io.strata.proto.v1.UpdateEvent;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.rocksdb.RocksDBException;

/**
 * This node's copy of one partition of one store.
 *
 * <p><b>Threading.</b> Reads run on any thread and only touch {@link #current} (an atomic pointer
 * plus a reference count). <i>Everything that writes</i>, whether applying stream updates,
 * replaying on a new version, or swapping, runs on one dedicated worker thread per partition. With
 * a single writer, the read-compare-write of last-writer-wins needs no locks and can't race with a
 * swap.
 *
 * <p><b>Version lifecycle</b> (ADR 0004):
 *
 * <ol>
 *   <li>{@link #prepare}: open a new RocksDB and ingest the version's SST files, off the worker.
 *   <li>Install, on the worker: replay log records that might be newer than the version's watermark
 *       into the new version (FULL mode), then keep applying live updates to <i>both</i> the
 *       current and the new version.
 *   <li>{@link #swap}, on the worker: flip the pointer. Readers move to the new version on their
 *       next read, and the old one closes when its last reader leaves.
 * </ol>
 */
public final class PartitionReplica implements AutoCloseable {

  private static final int READ_BATCH = 1000;

  private final String store;
  private final int partition;
  private final MergeMode mode;
  private final UpdateLog log; // nullable: a batch-only store
  private final long maxClockSkewMs;
  private final RocksResources resources;
  private final Path dir;

  private final AtomicReference<PartitionVersion> current = new AtomicReference<>();
  private final LinkedBlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
  private final Thread worker;
  private volatile boolean running = true;

  // Worker-confined state: only the worker thread reads or writes these.
  private PartitionVersion future;
  private volatile long preparedVersion = -1; // mirrors future's version, readable by any thread
  private long nextOffset = -1; // -1: not consuming the log yet (no version installed)

  // Metrics (read from any thread).
  private volatile long consumedOffset = -1;
  private final AtomicLong applied = new AtomicLong();
  private final AtomicLong droppedBelowWatermark = new AtomicLong();
  private final AtomicLong droppedOlder = new AtomicLong();
  private volatile long lastReplayRecords;
  private volatile long lastReplayMicros;

  PartitionReplica(
      String store,
      int partition,
      MergeMode mode,
      UpdateLog log,
      long maxClockSkewMs,
      RocksResources resources,
      Path dir) {
    this.store = store;
    this.partition = partition;
    this.mode = mode;
    this.log = log;
    this.maxClockSkewMs = maxClockSkewMs;
    this.resources = resources;
    this.dir = dir;
    this.worker = new Thread(this::runWorker, "partition-" + store + "-" + partition);
    this.worker.setDaemon(true);
    this.worker.start();
  }

  public int partition() {
    return partition;
  }

  // ---------------------------------------------------------------- reads (any thread)

  public Lookup get(byte[] key) throws NotServingException {
    while (true) {
      PartitionVersion v = current.get();
      if (v == null) {
        throw new NotServingException("partition " + partition + " of " + store + " not ready");
      }
      if (!v.tryAcquire()) {
        continue; // swapped and closed between get() and tryAcquire(): retry on the new version
      }
      try {
        byte[] raw = v.getRaw(key);
        if (raw == null) {
          return Lookup.missing(v.version());
        }
        StoredValue sv = StoredValue.decode(raw);
        return sv.tombstone()
            ? Lookup.missing(v.version())
            : new Lookup(true, sv.value(), v.version());
      } catch (RocksDBException e) {
        throw new IllegalStateException("read failed", e);
      } finally {
        v.release();
      }
    }
  }

  /** Version currently served, or -1. */
  public long currentVersion() {
    PartitionVersion v = current.get();
    return v == null ? -1 : v.version();
  }

  /** Version loaded, caught up and waiting for {@link #swap}, or -1. */
  public long preparedVersion() {
    return preparedVersion;
  }

  /** How many log records behind the end of the log this replica is (0 if no log). */
  public long lag() {
    if (log == null || consumedOffset < 0) {
      return 0;
    }
    return Math.max(0, log.endOffset(partition) - consumedOffset);
  }

  public long applied() {
    return applied.get();
  }

  public long droppedBelowWatermark() {
    return droppedBelowWatermark.get();
  }

  public long droppedOlder() {
    return droppedOlder.get();
  }

  public long lastReplayRecords() {
    return lastReplayRecords;
  }

  public long lastReplayMicros() {
    return lastReplayMicros;
  }

  // ---------------------------------------------------------------- version lifecycle

  /**
   * Loads {@code files} as {@code version} and installs it as the future version. Completes when
   * the future version is caught up with the log and ready to {@link #swap}.
   */
  public CompletableFuture<Void> prepare(
      long version, long watermark, List<Path> files, boolean compact, Executor loader) {
    Path vdir = dir.resolve("v" + version + "-" + System.nanoTime());
    return CompletableFuture.supplyAsync(
            () -> {
              try {
                PartitionVersion v = PartitionVersion.create(resources, version, watermark, vdir);
                try {
                  v.ingest(files);
                  if (compact) {
                    v.compact();
                  }
                } catch (IOException | RuntimeException e) {
                  v.retire();
                  throw e;
                }
                return v;
              } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
              }
            },
            loader)
        .thenCompose(v -> onWorker(() -> install(v)));
  }

  /** Makes the prepared {@code version} the one that serves reads. */
  public CompletableFuture<Void> swap(long version) {
    return onWorker(
        () -> {
          if (future == null || future.version() != version) {
            throw new IllegalStateException(
                "partition %d: version %d is not prepared (future=%s)"
                    .formatted(partition, version, future));
          }
          PartitionVersion old = current.getAndSet(future);
          future = null;
          preparedVersion = -1;
          if (old != null) {
            old.retire();
          }
          return null;
        });
  }

  // ---------------------------------------------------------------- worker

  private <T> CompletableFuture<T> onWorker(Supplier<T> body) {
    CompletableFuture<T> f = new CompletableFuture<>();
    tasks.add(
        () -> {
          try {
            f.complete(body.get());
          } catch (Throwable t) {
            f.completeExceptionally(t);
          }
        });
    return f;
  }

  private Void install(PartitionVersion v) {
    if (future != null) {
      future.retire(); // a newer push supersedes one that was prepared but never swapped
    }
    if (log != null) {
      PartitionVersion cur = current.get();
      // Any update with ts > watermark was appended no earlier than (watermark - skew), so
      // replaying from there is guaranteed to cover every update the snapshot doesn't contain.
      long replayFrom = log.offsetForTime(partition, v.watermark() - maxClockSkewMs);
      long replayTo;
      if (cur == null) {
        replayTo = log.endOffset(partition); // bootstrap: catch up, then consume live from here
      } else if (mode == MergeMode.FULL) {
        replayTo = nextOffset; // the current version already saw everything before nextOffset
      } else {
        replayTo = replayFrom; // NAIVE / LWW: no replay; updates between cutoff and now are lost
      }
      long start = System.nanoTime();
      long n = replay(v, replayFrom, replayTo);
      lastReplayRecords = n;
      lastReplayMicros = (System.nanoTime() - start) / 1000;
      if (cur == null) {
        nextOffset = Math.max(replayTo, replayFrom);
        consumedOffset = nextOffset;
      }
    }
    future = v;
    preparedVersion = v.version();
    return null;
  }

  private long replay(PartitionVersion v, long from, long to) {
    long n = 0;
    long off = from;
    try {
      while (off < to) {
        List<LogRecord> batch =
            log.read(partition, off, (int) Math.min(READ_BATCH, to - off), Duration.ZERO);
        if (batch.isEmpty()) {
          break;
        }
        for (LogRecord r : batch) {
          if (r.offset() >= to) {
            break;
          }
          apply(v, r.event());
          off = r.offset() + 1;
          n++;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return n;
  }

  private void runWorker() {
    while (running) {
      try {
        Runnable t;
        while ((t = tasks.poll()) != null) {
          t.run();
        }
        if (log != null && nextOffset >= 0) {
          List<LogRecord> batch = log.read(partition, nextOffset, READ_BATCH, Duration.ofMillis(5));
          for (LogRecord r : batch) {
            try {
              PartitionVersion cur = current.get();
              if (cur != null) {
                apply(cur, r.event());
              }
              if (future != null) {
                apply(future, r.event()); // dual-write keeps the prepared version caught up
              }
            } finally {
              nextOffset = r.offset() + 1; // a poison record is skipped, not retried forever
            }
          }
          consumedOffset = nextOffset;
        } else {
          t = tasks.poll(5, TimeUnit.MILLISECONDS);
          if (t != null) {
            t.run();
          }
        }
      } catch (InterruptedException e) {
        return;
      } catch (RuntimeException e) {
        // Never let one bad record kill the partition; surface it and keep going.
        System.err.println("partition " + store + "/" + partition + " worker error: " + e);
      }
    }
  }

  /** The merge rule, per mode. Runs only on the worker thread. */
  private void apply(PartitionVersion v, UpdateEvent e) {
    byte[] key = e.getKey().toByteArray();
    StoredValue incoming =
        e.getDelete()
            ? StoredValue.tombstone(e.getTs(), e.getSourceId())
            : StoredValue.live(e.getTs(), e.getSourceId(), e.getValue().toByteArray());
    try {
      switch (mode) {
        case NAIVE -> {
          if (e.getDelete()) {
            v.deleteRaw(key);
          } else {
            v.putRaw(key, incoming.encode());
          }
        }
        case LWW -> mergeLww(v, key, incoming);
        case FULL -> {
          if (e.getTs() <= v.watermark()) {
            // The snapshot already reflects everything up to its watermark, including this
            // update or something newer. Applying it could only regress the value.
            droppedBelowWatermark.incrementAndGet();
            return;
          }
          mergeLww(v, key, incoming);
        }
      }
      applied.incrementAndGet();
    } catch (RocksDBException ex) {
      throw new IllegalStateException("write failed", ex);
    }
  }

  private void mergeLww(PartitionVersion v, byte[] key, StoredValue incoming)
      throws RocksDBException {
    byte[] existing = v.getRaw(key);
    if (existing != null && StoredValue.decode(existing).compareTo(incoming) >= 0) {
      droppedOlder.incrementAndGet();
      return;
    }
    // Deletes are stored as tombstones (not removed) so an older update arriving later can't
    // bring the key back to life.
    v.putRaw(key, incoming.encode());
  }

  @Override
  public void close() {
    running = false;
    worker.interrupt();
    try {
      worker.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    PartitionVersion f = future;
    if (f != null) {
      f.retire();
    }
    PartitionVersion c = current.getAndSet(null);
    if (c != null) {
      c.retire();
    }
  }
}
