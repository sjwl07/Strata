package io.strata.push;

import io.strata.common.Manifests;
import io.strata.common.ObjectStore;
import io.strata.common.StoredValue;
import io.strata.proto.v1.FileRef;
import io.strata.proto.v1.PartitionFiles;
import io.strata.proto.v1.PushStats;
import io.strata.proto.v1.PushType;
import io.strata.proto.v1.VersionManifest;
import io.strata.sst.SstWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Builds a version of a store offline and publishes it to the object store. The steps mirror what a
 * Spark job would do (ADR 0001): hash-partition the rows with the store's {@link Partitioner}, sort
 * each partition by key, write one SST per partition, upload, then publish the manifest last (the
 * manifest's existence is what makes the version visible).
 *
 * <p>Every batch record is stamped with the version's watermark as its timestamp, so any stream
 * update newer than the snapshot cutoff beats it during merging.
 */
public final class PushJob {

  public record Config(
      String store, int numPartitions, ObjectStore objectStore, Path workDir, int parallelism) {}

  private final Config config;

  public PushJob(Config config) {
    this.config = config;
  }

  record Entry(byte[] key, byte[] value) {}

  private static final Comparator<Entry> BY_KEY = (a, b) -> Arrays.compareUnsigned(a.key, b.key);

  /** A FULL push: the complete dataset. */
  public VersionManifest pushFull(Dataset dataset, long version, long watermark)
      throws IOException {
    long start = System.nanoTime();
    List<PartitionResult> results =
        forEachPartition(p -> writeFullPartition(dataset, p, version, watermark));
    return publish(PushType.FULL, 0, version, watermark, results, start);
  }

  /**
   * A DELTA push: only what changed between {@code previous} (already published as {@code
   * baseVersion}) and {@code next}. Changed and new keys become upserts; keys missing from {@code
   * next} become deletes. Nodes build the new version by layering this on the base's files.
   */
  public VersionManifest pushDelta(
      Dataset previous, Dataset next, long baseVersion, long version, long watermark)
      throws IOException {
    long start = System.nanoTime();
    List<PartitionResult> results =
        forEachPartition(p -> writeDeltaPartition(previous, next, p, version, watermark));
    return publish(PushType.DELTA, baseVersion, version, watermark, results, start);
  }

  record PartitionResult(int partition, FileRef file, long upserts, long deletes) {}

  @FunctionalInterface
  interface PartitionTask {
    PartitionResult run(int partition) throws IOException;
  }

  private List<PartitionResult> forEachPartition(PartitionTask task) throws IOException {
    ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, config.parallelism()));
    try {
      List<Future<PartitionResult>> futures = new ArrayList<>();
      for (int p = 0; p < config.numPartitions(); p++) {
        int partition = p;
        futures.add(pool.submit(() -> task.run(partition)));
      }
      List<PartitionResult> out = new ArrayList<>();
      for (Future<PartitionResult> f : futures) {
        out.add(f.get());
      }
      return out;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException io) {
        throw io;
      }
      throw new IOException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } finally {
      pool.shutdownNow();
    }
  }

  private List<Entry> collect(Dataset dataset, int partition) throws IOException {
    List<Entry> entries = new ArrayList<>();
    dataset.forEachInPartition(
        partition, config.numPartitions(), (k, v) -> entries.add(new Entry(k, v)));
    entries.sort(BY_KEY); // the "shuffle sort"; nearly free when the input is already sorted
    return entries;
  }

  private PartitionResult writeFullPartition(
      Dataset dataset, int partition, long version, long watermark) throws IOException {
    List<Entry> entries = collect(dataset, partition);
    Path local = config.workDir().resolve("v%d-p%d-full.sst".formatted(version, partition));
    try (SstWriter w = new SstWriter(local)) {
      for (Entry e : entries) {
        w.put(e.key, StoredValue.live(watermark, StoredValue.BATCH_SOURCE, e.value));
      }
      return upload(partition, version, "full", local, w, entries.size(), 0);
    }
  }

  private PartitionResult writeDeltaPartition(
      Dataset previous, Dataset next, int partition, long version, long watermark)
      throws IOException {
    // Merge-join two sorted runs. A real pipeline would compare per-row hashes kept from the last
    // run instead of re-reading the previous snapshot; the work is the same order of magnitude.
    List<Entry> old = collect(previous, partition);
    List<Entry> cur = collect(next, partition);
    Path local = config.workDir().resolve("v%d-p%d-delta.sst".formatted(version, partition));
    long upserts = 0;
    long deletes = 0;
    try (SstWriter w = new SstWriter(local)) {
      int i = 0;
      int j = 0;
      while (i < old.size() || j < cur.size()) {
        int c;
        if (i == old.size()) {
          c = 1;
        } else if (j == cur.size()) {
          c = -1;
        } else {
          c = Arrays.compareUnsigned(old.get(i).key, cur.get(j).key);
        }
        if (c < 0) { // only in old: deleted
          w.delete(old.get(i++).key);
          deletes++;
        } else if (c > 0) { // only in new: inserted
          Entry e = cur.get(j++);
          w.put(e.key, StoredValue.live(watermark, StoredValue.BATCH_SOURCE, e.value));
          upserts++;
        } else {
          Entry o = old.get(i++);
          Entry n = cur.get(j++);
          if (!Arrays.equals(o.value, n.value)) {
            w.put(n.key, StoredValue.live(watermark, StoredValue.BATCH_SOURCE, n.value));
            upserts++;
          }
        }
      }
      return upload(partition, version, "delta", local, w, upserts, deletes);
    }
  }

  private PartitionResult upload(
      int partition, long version, String kind, Path local, SstWriter w, long ups, long dels)
      throws IOException {
    long size = w.finish();
    if (size == 0) {
      return new PartitionResult(partition, null, 0, 0); // nothing changed in this partition
    }
    String key = Manifests.sstKey(config.store(), version, partition, kind);
    try {
      config.objectStore().put(key, local);
    } finally {
      Files.deleteIfExists(local);
    }
    FileRef ref =
        FileRef.newBuilder().setObjectKey(key).setSizeBytes(size).setEntries(w.entries()).build();
    return new PartitionResult(partition, ref, ups, dels);
  }

  private VersionManifest publish(
      PushType type,
      long baseVersion,
      long version,
      long watermark,
      List<PartitionResult> results,
      long startNanos)
      throws IOException {
    VersionManifest.Builder m =
        VersionManifest.newBuilder()
            .setStore(config.store())
            .setVersion(version)
            .setType(type)
            .setBaseVersion(baseVersion)
            .setWatermarkTs(watermark)
            .setNumPartitions(config.numPartitions());
    long ups = 0;
    long dels = 0;
    long bytes = 0;
    for (PartitionResult r : results) {
      PartitionFiles.Builder pf = PartitionFiles.newBuilder().setPartition(r.partition());
      if (r.file() != null) {
        pf.addFiles(r.file());
        bytes += r.file().getSizeBytes();
      }
      m.addPartitions(pf);
      ups += r.upserts();
      dels += r.deletes();
    }
    m.setStats(
        PushStats.newBuilder()
            .setRecordsWritten(ups + dels)
            .setUpserts(ups)
            .setDeletes(dels)
            .setBytesWritten(bytes)
            .setBuildMillis((System.nanoTime() - startNanos) / 1_000_000));
    VersionManifest manifest = m.build();
    Manifests.publish(config.objectStore(), manifest);
    return manifest;
  }
}
