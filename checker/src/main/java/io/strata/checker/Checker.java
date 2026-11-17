package io.strata.checker;

import io.strata.checker.History.Action;
import io.strata.checker.History.Append;
import io.strata.checker.History.Check;
import io.strata.checker.History.Event;
import io.strata.checker.History.Push;
import io.strata.checker.History.Restart;
import io.strata.checker.History.Swap;
import io.strata.common.InMemoryUpdateLog;
import io.strata.common.LocalObjectStore;
import io.strata.common.Partitioner;
import io.strata.node.engine.FileCache;
import io.strata.node.engine.Files2;
import io.strata.node.engine.Lookup;
import io.strata.node.engine.MergeMode;
import io.strata.node.engine.NotServingException;
import io.strata.node.engine.PartitionReplica;
import io.strata.node.engine.RocksResources;
import io.strata.node.engine.StoreEngine;
import io.strata.push.MapDataset;
import io.strata.push.PushJob;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays a {@link History} against two real replicas (A and B) of a store, each consuming the same
 * log, and compares every read with the {@link Oracle}. Replica B is crashed and rebuilt when the
 * history says so, to exercise bootstrap.
 */
public final class Checker implements AutoCloseable {

  /** Kinds of wrong answers. */
  public enum Anomaly {
    /** Served an older value than the newest the store could know (e.g. a swap lost an update). */
    STALE,
    /** Served a value although the key is deleted (or never existed as of the cutoff). */
    RESURRECTED,
    /** Served "not found" although the key has a live value. */
    LOST,
    /** A later read of the key returned an older value than an earlier read (time went back). */
    REGRESSED,
    /** Replicas A and B disagree at the end of the history. */
    DIVERGED
  }

  /** Totals over one or more histories. */
  public static final class Report {
    public long histories;
    public long reads;
    public long wrongReads;
    public final EnumMap<Anomaly, Long> anomalies = new EnumMap<>(Anomaly.class);
    public long replayedRecords;
    public long replayMicros;
    public long swaps;
    public final List<Long> failingSeeds = new ArrayList<>();

    void add(Anomaly a) {
      anomalies.merge(a, 1L, Long::sum);
    }

    public long count(Anomaly a) {
      return anomalies.getOrDefault(a, 0L);
    }

    public void merge(Report o) {
      histories += o.histories;
      reads += o.reads;
      wrongReads += o.wrongReads;
      o.anomalies.forEach((k, v) -> anomalies.merge(k, v, Long::sum));
      replayedRecords += o.replayedRecords;
      replayMicros += o.replayMicros;
      swaps += o.swaps;
      failingSeeds.addAll(o.failingSeeds);
    }

    public long totalAnomalies() {
      return anomalies.values().stream().mapToLong(Long::longValue).sum();
    }
  }

  private static final int PARTITIONS = 2;

  private final Path workDir;
  private final RocksResources resources = new RocksResources(16L << 20, 16L << 20);
  private final ExecutorService loader = Executors.newFixedThreadPool(2);

  public Checker(Path workDir) {
    this.workDir = workDir;
  }

  static String keyName(int k) {
    return "key-%03d".formatted(k);
  }

  static byte[] keyBytes(int k) {
    return keyName(k).getBytes(StandardCharsets.UTF_8);
  }

  public Report run(History h, MergeMode mode, long engineSkewMs) throws Exception {
    Path dir = workDir.resolve("h" + h.seed() + "-" + mode + "-" + engineSkewMs);
    Files2.deleteRecursively(dir);
    Report report = new Report();
    report.histories = 1;
    List<Event> allEvents = new ArrayList<>();
    for (Action a : h.actions()) {
      if (a instanceof Append ap && !ap.duplicate()) {
        allEvents.add(ap.event());
      }
    }
    Oracle oracle = new Oracle(allEvents);
    AtomicLong clock = new AtomicLong();
    InMemoryUpdateLog log = new InMemoryUpdateLog(PARTITIONS, clock::get);
    LocalObjectStore objects = new LocalObjectStore(dir.resolve("objects"));
    PushJob push =
        new PushJob(new PushJob.Config("c", PARTITIONS, objects, dir.resolve("push"), 1));
    StoreEngine.Config cfg = new StoreEngine.Config("c", PARTITIONS, mode, engineSkewMs, false);
    Map<Long, Long> watermarks = new HashMap<>();
    long[] lastSwapped = {0};
    long[] pending = {0};
    int[] incarnation = {0};

    StoreEngine a = newEngine(cfg, dir.resolve("A"), objects, log);
    StoreEngine[] b = {newEngine(cfg, dir.resolve("B0"), objects, log)};
    Map<String, long[]> lastSeenTs = new HashMap<>();
    try {
      for (Action act : h.actions()) {
        clock.set(Math.max(clock.get(), act.at()));
        switch (act) {
          case Append ap -> {
            log.append(
                Partitioner.partition(keyBytes(ap.event().key()), PARTITIONS),
                ap.event().toProto(keyBytes(ap.event().key())));
            oracle.delivered(ap.event());
          }
          case Push p -> {
            Map<String, byte[]> rows = new HashMap<>();
            oracle.snapshot(p.watermark()).forEach((k, e) -> rows.put(keyName(k), e.valueBytes()));
            push.pushFull(new MapDataset(rows), p.version(), p.watermark());
            watermarks.put(p.version(), p.watermark());
            a.prepare(p.version()).get(60, TimeUnit.SECONDS);
            b[0].prepare(p.version()).get(60, TimeUnit.SECONDS);
            pending[0] = p.version();
            for (int part = 0; part < PARTITIONS; part++) {
              PartitionReplica r = a.replica(part);
              report.replayedRecords += r.lastReplayRecords();
              report.replayMicros += r.lastReplayMicros();
            }
          }
          case Swap s -> {
            if (pending[0] == s.version()) {
              a.swap(s.version()).get(60, TimeUnit.SECONDS);
              b[0].swap(s.version()).get(60, TimeUnit.SECONDS);
              lastSwapped[0] = s.version();
              pending[0] = 0;
              report.swaps++;
            }
          }
          case Restart r -> {
            b[0].close();
            incarnation[0]++;
            b[0] = newEngine(cfg, dir.resolve("B" + incarnation[0]), objects, log);
            b[0].load(lastSwapped[0]).get(60, TimeUnit.SECONDS);
            if (pending[0] != 0) {
              b[0].prepare(pending[0]).get(60, TimeUnit.SECONDS);
            }
            lastSeenTs.keySet().removeIf(k -> k.startsWith("B:")); // a fresh process
          }
          case Check c -> {
            awaitCaughtUp(a);
            awaitCaughtUp(b[0]);
            for (int k = 0; k < h.numKeys(); k++) {
              check(report, oracle, watermarks, a, "A", k, lastSeenTs);
              check(report, oracle, watermarks, b[0], "B", k, lastSeenTs);
            }
          }
        }
      }
      // Final divergence check: both replicas on the same version, fully caught up.
      awaitCaughtUp(a);
      awaitCaughtUp(b[0]);
      for (int k = 0; k < h.numKeys(); k++) {
        Lookup la = a.get(keyBytes(k));
        Lookup lb = b[0].get(keyBytes(k));
        if (la.version() == lb.version()
            && (la.found() != lb.found() || !Arrays.equals(la.value(), lb.value()))) {
          report.add(Anomaly.DIVERGED);
        }
      }
    } finally {
      a.close();
      b[0].close();
      log.close();
    }
    if (report.totalAnomalies() > 0) {
      report.failingSeeds.add(h.seed());
    }
    resources.awaitCleaner(); // bound open RocksDB instances across histories
    Files2.deleteRecursively(dir);
    return report;
  }

  private void check(
      Report report,
      Oracle oracle,
      Map<Long, Long> watermarks,
      StoreEngine replica,
      String name,
      int k,
      Map<String, long[]> lastSeenTs)
      throws NotServingException {
    Lookup got = replica.get(keyBytes(k));
    report.reads++;
    long w = watermarks.get(got.version());
    Event expected = oracle.expected(k, w);
    boolean expectFound = expected != null && !expected.delete();
    Event actual = got.found() ? parse(got.value(), oracle) : null;
    boolean wrong = false;
    if (expectFound && actual == null) {
      report.add(Anomaly.LOST);
      wrong = true;
    } else if (!expectFound && actual != null) {
      report.add(Anomaly.RESURRECTED);
      wrong = true;
    } else if (expectFound && actual.id() != expected.id()) {
      report.add(Anomaly.STALE);
      wrong = true;
    }
    if (wrong) {
      report.wrongReads++;
    }
    if (actual != null) {
      long[] last = lastSeenTs.computeIfAbsent(name + ":" + k, x -> new long[] {Long.MIN_VALUE});
      if (actual.ts() < last[0]) {
        report.add(Anomaly.REGRESSED);
      }
      last[0] = Math.max(last[0], actual.ts());
    }
  }

  private Event parse(byte[] value, Oracle oracle) {
    int id = Integer.parseInt(new String(value, StandardCharsets.US_ASCII).substring(1));
    return oracle.byId(id);
  }

  private static void awaitCaughtUp(StoreEngine e) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (e.totalLag() > 0) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("replica did not catch up");
      }
      Thread.sleep(0, 200_000);
    }
  }

  private StoreEngine newEngine(
      StoreEngine.Config cfg, Path dir, LocalObjectStore objects, InMemoryUpdateLog log)
      throws Exception {
    StoreEngine e =
        new StoreEngine(
            cfg,
            resources,
            dir.resolve("data"),
            objects,
            new FileCache(dir.resolve("cache"), objects),
            log,
            loader);
    for (int p = 0; p < PARTITIONS; p++) {
      e.host(p);
    }
    return e;
  }

  @Override
  public void close() {
    loader.shutdownNow();
    resources.close();
  }
}
