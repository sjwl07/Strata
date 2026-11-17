package io.strata.node.engine;

import io.strata.common.Manifests;
import io.strata.common.ObjectStore;
import io.strata.common.Partitioner;
import io.strata.common.UpdateLog;
import io.strata.proto.v1.FileRef;
import io.strata.proto.v1.VersionManifest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * One store on one node: the partitions this node hosts, plus the logic to turn a pushed version (a
 * manifest in the object store) into prepared, then swapped, partition versions.
 */
public final class StoreEngine implements AutoCloseable {

  /**
   * @param maxClockSkewMs how far an update's event time may be ahead of its log append time;
   *     widens the replay window (ADR 0004)
   * @param compactAfterIngest fully compact each partition after loading a version (Q1 knob)
   */
  public record Config(
      String name,
      int numPartitions,
      MergeMode mode,
      long maxClockSkewMs,
      boolean compactAfterIngest) {
    public Config(String name, int numPartitions) {
      this(name, numPartitions, MergeMode.FULL, 1000, false);
    }
  }

  private final Config config;
  private final RocksResources resources;
  private final Path dir;
  private final ObjectStore objectStore;
  private final FileCache fileCache;
  private final UpdateLog log;
  private final Executor loader;
  private final ConcurrentHashMap<Integer, PartitionReplica> partitions = new ConcurrentHashMap<>();
  // File chains of versions currently prepared or served, per partition (for cache pruning).
  private final ConcurrentHashMap<Integer, Map<Long, List<String>>> chains =
      new ConcurrentHashMap<>();

  public StoreEngine(
      Config config,
      RocksResources resources,
      Path dir,
      ObjectStore objectStore,
      FileCache fileCache,
      UpdateLog log,
      Executor loader) {
    if (log != null && log.numPartitions() != config.numPartitions()) {
      throw new IllegalArgumentException("log and store must have the same partition count");
    }
    this.config = config;
    this.resources = resources;
    this.dir = dir;
    this.objectStore = objectStore;
    this.fileCache = fileCache;
    this.log = log;
    this.loader = loader;
  }

  public Config config() {
    return config;
  }

  public String name() {
    return config.name();
  }

  public int numPartitions() {
    return config.numPartitions();
  }

  /** Starts hosting {@code partition} (no data until a version is prepared and swapped). */
  public PartitionReplica host(int partition) {
    return partitions.computeIfAbsent(
        partition,
        p ->
            new PartitionReplica(
                config.name(),
                p,
                config.mode(),
                log,
                config.maxClockSkewMs(),
                resources,
                dir.resolve("p" + p)));
  }

  public void unhost(int partition) {
    PartitionReplica r = partitions.remove(partition);
    chains.remove(partition);
    if (r != null) {
      r.close();
    }
  }

  public Set<Integer> hosted() {
    return Set.copyOf(partitions.keySet());
  }

  public PartitionReplica replica(int partition) {
    return partitions.get(partition);
  }

  public Lookup get(byte[] key) throws NotServingException {
    int p = Partitioner.partition(key, config.numPartitions());
    PartitionReplica r = partitions.get(p);
    if (r == null) {
      throw new NotServingException("partition " + p + " of " + config.name() + " not hosted");
    }
    return r.get(key);
  }

  /** Prepares {@code version} on every hosted partition. */
  public CompletableFuture<Void> prepare(long version) {
    return prepare(version, hosted());
  }

  /**
   * Downloads (or reuses) each partition's file chain and loads it as a future version. Completes
   * when every listed partition is caught up and ready to swap.
   */
  public CompletableFuture<Void> prepare(long version, Collection<Integer> which) {
    VersionManifest m;
    try {
      m = Manifests.load(objectStore, config.name(), version);
    } catch (IOException e) {
      return CompletableFuture.failedFuture(e);
    }
    if (m.getNumPartitions() != config.numPartitions()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("manifest partition count mismatch"));
    }
    List<CompletableFuture<Void>> all = new ArrayList<>();
    for (int p : which) {
      PartitionReplica r = host(p);
      all.add(
          CompletableFuture.supplyAsync(() -> fetchChain(m, p), loader)
              .thenCompose(
                  files ->
                      r.prepare(
                          version,
                          m.getWatermarkTs(),
                          files,
                          config.compactAfterIngest(),
                          loader)));
    }
    return CompletableFuture.allOf(all.toArray(CompletableFuture[]::new));
  }

  private List<Path> fetchChain(VersionManifest m, int partition) {
    try {
      List<FileRef> chain = Manifests.fileChain(objectStore, m, partition);
      List<Path> out = new ArrayList<>();
      for (FileRef f : chain) {
        out.add(fileCache.fetch(f));
      }
      chains
          .computeIfAbsent(partition, k -> new ConcurrentHashMap<>())
          .put(m.getVersion(), chain.stream().map(FileRef::getObjectKey).toList());
      return out;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public CompletableFuture<Void> swap(long version) {
    return swap(version, hosted());
  }

  public CompletableFuture<Void> swap(long version, Collection<Integer> which) {
    List<CompletableFuture<Void>> all = new ArrayList<>();
    for (int p : which) {
      PartitionReplica r = partitions.get(p);
      if (r != null) {
        all.add(
            r.swap(version)
                .thenRun(
                    () -> {
                      Map<Long, List<String>> c = chains.get(p);
                      if (c != null) {
                        // After a swap only the served version's files are needed (a prepare
                        // racing with this swap re-registers its chain when it fetches).
                        c.keySet().removeIf(v -> v != version);
                      }
                    }));
      }
    }
    return CompletableFuture.allOf(all.toArray(CompletableFuture[]::new));
  }

  /** Prepare then swap, for a standalone node that has no controller. */
  public CompletableFuture<Void> load(long version) {
    return prepare(version).thenCompose(v -> swap(version));
  }

  /** Object keys still referenced by a prepared or served version of this store. */
  public Set<String> referencedFiles() {
    Set<String> out = new HashSet<>();
    chains.values().forEach(m -> m.values().forEach(out::addAll));
    return out;
  }

  /** Served version per hosted partition (-1 = not ready). */
  public Map<Integer, Long> currentVersions() {
    Map<Integer, Long> out = new TreeMap<>();
    partitions.forEach((p, r) -> out.put(p, r.currentVersion()));
    return out;
  }

  public long totalLag() {
    return partitions.values().stream().mapToLong(PartitionReplica::lag).sum();
  }

  @Override
  public void close() {
    partitions.values().forEach(PartitionReplica::close);
    partitions.clear();
  }
}
