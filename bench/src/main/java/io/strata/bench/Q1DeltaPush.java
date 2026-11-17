package io.strata.bench;

import io.strata.common.LocalObjectStore;
import io.strata.common.Manifests;
import io.strata.node.StorageNode;
import io.strata.node.engine.Files2;
import io.strata.node.engine.Lookup;
import io.strata.node.engine.StoreEngine;
import io.strata.proto.v1.VersionManifest;
import io.strata.push.PushJob;
import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.HdrHistogram.Histogram;

/**
 * Q1: when a fraction f of keys changed since the last snapshot, does shipping a delta beat a full
 * re-push? For each f, both kinds are built from the same base and loaded onto the same node:
 *
 * <ul>
 *   <li>push-job build time and bytes written (= bytes shipped);
 *   <li>node prepare time (download into the file cache + ingest) and bytes actually downloaded;
 *   <li>modelled transfer time at a configurable object-store bandwidth (local copies are faster
 *       than any network, so the measured number alone would flatter big pushes);
 *   <li>point-read cost on the new version, in-process (no RPC) so read amplification from layered
 *       files is visible, plus the read tail <i>during</i> the load.
 * </ul>
 *
 * Then a chain sweep: k consecutive 1% deltas without compaction, to see read cost grow with k.
 */
final class Q1DeltaPush {
  private Q1DeltaPush() {}

  static void run(Args a) throws Exception {
    long keys = a.l("keys", 500_000);
    int valueSize = a.i("valueSize", 100);
    int partitions = a.i("partitions", 8);
    double bandwidthMBps = a.d("bandwidthMBps", 125); // ~1 Gbit/s
    double deleteShare = a.d("deleteShare", 0.1);
    Path work = Path.of(a.s("work", "build/experiments/q1")).toAbsolutePath();
    Path out = Path.of(a.s("out", "../docs/experiments/data")).toAbsolutePath();
    Files2.deleteRecursively(work);

    LocalObjectStore objects = new LocalObjectStore(work.resolve("objects"));
    PushJob push =
        new PushJob(new PushJob.Config("q1", partitions, objects, work.resolve("push"), 2));
    SyntheticDataset s0 = SyntheticDataset.initial(keys, valueSize, 11);
    VersionManifest base = push.pushFull(s0, 1, 1_000);

    List<String> rows = new ArrayList<>();
    try (StorageNode node =
        new StorageNode(StorageNode.Config.local("q1", work.resolve("node")), objects)) {
      StoreEngine store = node.addStore(new StoreEngine.Config("q1", partitions), null);
      for (int p = 0; p < partitions; p++) {
        store.host(p);
      }
      store.load(1).get(10, TimeUnit.MINUTES);
      warm(store, keys);
      long version = 2;
      for (double f : parseList(a.s("fractions", "0.001,0.01,0.05,0.1,0.25,0.5,1.0"))) {
        SyntheticDataset s1 = s0.next(f, deleteShare);
        for (String kind : List.of("delta", "full")) {
          long v = version++;
          long t = System.nanoTime();
          VersionManifest m =
              kind.equals("full")
                  ? push.pushFull(s1, v, 2_000)
                  : push.pushDelta(s0, s1, 1, v, 2_000);
          double buildMs = (System.nanoTime() - t) / 1e6;
          long bytes = m.getStats().getBytesWritten();
          long before = node.fileCache().bytesDownloaded();
          Histogram during = new Histogram(3);
          t = System.nanoTime();
          readWhile(store, keys, store.load(v), during);
          double prepareMs = (System.nanoTime() - t) / 1e6;
          long downloaded = node.fileCache().bytesDownloaded() - before;
          Histogram after = readCost(store, keys, 200_000, 7);
          double transferMs = bytes / (bandwidthMBps * 1e6) * 1e3;
          String row =
              "%.4f,%s,%d,%d,%.1f,%.1f,%.1f,%.1f,%d,%.0f,%.0f,%.0f"
                  .formatted(
                      f,
                      kind,
                      m.getStats().getRecordsWritten(),
                      bytes,
                      buildMs,
                      transferMs,
                      prepareMs,
                      buildMs + transferMs + prepareMs,
                      downloaded,
                      after.getMean(),
                      (double) after.getValueAtPercentile(99),
                      (double) during.getValueAtPercentile(99.9));
          rows.add(row);
          System.out.println(row);
          // Back to the base so the next delta applies to the same starting point, then drop
          // this version's files (disk is tight on the test machine).
          store.load(1).get(10, TimeUnit.MINUTES);
          deleteVersion(objects, v);
          node.pruneFileCache();
        }
      }
      Results.csv(
          out,
          "q1-delta-vs-full.csv",
          "fraction,kind,records,bytes,build_ms,transfer_ms_model,prepare_ms,total_ms,"
              + "downloaded_bytes,read_mean_ns,read_p99_ns,read_p999_during_load_ns",
          rows);

      // Chain sweep: k successive 1% deltas, no compaction, then one full compaction.
      List<String> chain = new ArrayList<>();
      SyntheticDataset prev = s0;
      long prevVersion = 1;
      Histogram h0 = readCost(store, keys, 200_000, 9);
      chain.add("0,%.0f,%.0f".formatted(h0.getMean(), (double) h0.getValueAtPercentile(99)));
      for (int k = 1; k <= a.i("chain", 8); k++) {
        SyntheticDataset next = prev.next(0.01, deleteShare);
        long v = 1000 + k;
        push.pushDelta(prev, next, prevVersion, v, 2_000 + k);
        store.load(v).get(10, TimeUnit.MINUTES);
        Histogram h = readCost(store, keys, 200_000, 9);
        chain.add("%d,%.0f,%.0f".formatted(k, h.getMean(), (double) h.getValueAtPercentile(99)));
        System.out.printf(
            "chain k=%d read mean=%.0fns p99=%dns%n", k, h.getMean(), h.getValueAtPercentile(99));
        prev = next;
        prevVersion = v;
      }
      // Same chain, compacted after ingest, on a second node.
      try (StorageNode node2 =
          new StorageNode(StorageNode.Config.local("q1c", work.resolve("node2")), objects)) {
        StoreEngine compacted =
            node2.addStore(
                new StoreEngine.Config("q1", partitions, store.config().mode(), 1000, true), null);
        for (int p = 0; p < partitions; p++) {
          compacted.host(p);
        }
        long t = System.nanoTime();
        compacted.load(prevVersion).get(10, TimeUnit.MINUTES);
        double ms = (System.nanoTime() - t) / 1e6;
        Histogram hc = readCost(compacted, keys, 200_000, 9);
        chain.add(
            "compacted,%.0f,%.0f".formatted(hc.getMean(), (double) hc.getValueAtPercentile(99)));
        System.out.printf(
            "chain compacted: load+compact %.0f ms, read mean=%.0fns%n", ms, hc.getMean());
      }
      Results.csv(out, "q1-chain.csv", "deltas_in_chain,read_mean_ns,read_p99_ns", chain);
    }
    Files2.deleteRecursively(work);
  }

  private static void deleteVersion(LocalObjectStore objects, long v) throws Exception {
    Files2.deleteRecursively(objects.root().resolve(Manifests.versionPrefix("q1", v)));
  }

  private static void warm(StoreEngine store, long keys) throws Exception {
    readCost(store, keys, 100_000, 1);
  }

  /** Point reads of random existing-or-not keys; nanoseconds per read. */
  static Histogram readCost(StoreEngine store, long keys, int n, long seed) throws Exception {
    Histogram h = new Histogram(3);
    SplittableRandom r = new SplittableRandom(seed);
    for (int i = 0; i < n; i++) {
      byte[] k = SyntheticDataset.key(r.nextLong(keys));
      long s = System.nanoTime();
      Lookup l = store.get(k);
      h.recordValue(System.nanoTime() - s);
      if (l.version() < 0) {
        throw new IllegalStateException();
      }
    }
    return h;
  }

  private static void readWhile(
      StoreEngine store, long keys, CompletableFuture<Void> task, Histogram into) throws Exception {
    AtomicBoolean done = new AtomicBoolean();
    task.whenComplete((x, e) -> done.set(true));
    SplittableRandom r = new SplittableRandom(3);
    while (!done.get()) {
      byte[] k = SyntheticDataset.key(r.nextLong(keys));
      long s = System.nanoTime();
      store.get(k);
      into.recordValue(System.nanoTime() - s);
    }
    task.get(10, TimeUnit.MINUTES);
  }

  private static double[] parseList(String s) {
    String[] parts = s.split(",");
    double[] d = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      d[i] = Double.parseDouble(parts[i]);
    }
    return d;
  }
}
