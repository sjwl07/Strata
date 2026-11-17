package io.strata.bench;

import com.google.protobuf.ByteString;
import io.strata.checker.Checker.Report;
import io.strata.checker.CheckerMain;
import io.strata.checker.HistoryGenerator;
import io.strata.common.InMemoryUpdateLog;
import io.strata.common.LocalObjectStore;
import io.strata.node.engine.FileCache;
import io.strata.node.engine.Files2;
import io.strata.node.engine.MergeMode;
import io.strata.node.engine.PartitionReplica;
import io.strata.node.engine.RocksResources;
import io.strata.node.engine.StoreEngine;
import io.strata.proto.v1.UpdateEvent;
import io.strata.push.PushJob;
import io.strata.push.SyntheticDataset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Q2: how often does each merge strategy serve wrong data, and what does correctness cost?
 *
 * <ol>
 *   <li>Correctness: random histories through the checker, per mode.
 *   <li>Sensitivity: the cutoff-to-swap delay vs the wrong-read rate of LWW-without-replay.
 *   <li>Cost: stream-apply throughput per mode, and replay time vs records since the watermark.
 * </ol>
 */
final class Q2MergeCorrectness {
  private Q2MergeCorrectness() {}

  static void run(Args a) throws Exception {
    int histories = a.i("histories", 100);
    Path work = Path.of(a.s("work", "build/experiments/q2")).toAbsolutePath();
    Path out = Path.of(a.s("out", "../docs/experiments/data")).toAbsolutePath();
    Files2.deleteRecursively(work);

    if (a.s("only", "").equals("cost")) {
      cost(work.resolve("cost"), out, a.l("costKeys", 200_000), a.i("costEvents", 300_000));
      Files2.deleteRecursively(work);
      return;
    }
    // 1. Correctness table.
    HistoryGenerator.Params params = HistoryGenerator.Params.defaults();
    long t = System.nanoTime();
    Map<String, Report> main =
        CheckerMain.run(
            work.resolve("main"), params, histories, 1, "FULL", "FULL-noskew", "LWW", "NAIVE");
    String table = CheckerMain.table(main);
    System.out.println(table);
    System.out.printf("checker time: %.0f s%n", (System.nanoTime() - t) / 1e9);
    Files.createDirectories(out);
    Files.writeString(out.resolve("q2-correctness.md"), table);

    // 2. Sensitivity to the time between snapshot cutoff and swap (LWW and NAIVE).
    List<String> rows = new ArrayList<>();
    int sweepHistories = Math.max(10, histories / 4);
    for (int delay : new int[] {250, 1000, 2000, 4000}) {
      HistoryGenerator.Params p =
          new HistoryGenerator.Params(
              params.numKeys(),
              params.numEvents(),
              params.meanGapMs(),
              params.lateProbability(),
              params.maxLatenessMs(),
              params.skewProbability(),
              params.maxSkewMs(),
              params.duplicateProbability(),
              params.deleteProbability(),
              params.numSources(),
              Math.max(params.pushEveryMs(), delay + 500),
              delay / 2 + 1,
              delay,
              params.maxSwapDelayMs(),
              params.checkEveryMs(),
              params.restartProbabilityPerPush());
      Map<String, Report> r =
          CheckerMain.run(
              work.resolve("sweep" + delay), p, sweepHistories, 10_000, "FULL", "LWW", "NAIVE");
      for (var e : r.entrySet()) {
        Report rep = e.getValue();
        rows.add(
            "%d,%s,%d,%d,%.4f"
                .formatted(
                    delay,
                    e.getKey(),
                    rep.reads,
                    rep.wrongReads,
                    (double) rep.wrongReads / rep.reads));
        System.out.printf(
            "maxBuildDelay=%dms %s wrong=%.3f%%%n",
            delay, e.getKey(), 100.0 * rep.wrongReads / rep.reads);
      }
    }
    Results.csv(
        out, "q2-delay-sweep.csv", "max_cutoff_to_swap_ms,mode,reads,wrong,wrong_frac", rows);

    // 3. Cost.
    cost(work.resolve("cost"), out, a.l("costKeys", 200_000), a.i("costEvents", 300_000));
    Files2.deleteRecursively(work);
  }

  private static void cost(Path work, Path out, long keys, int events) throws Exception {
    List<String> rows = new ArrayList<>();
    LocalObjectStore objects = new LocalObjectStore(work.resolve("objects"));
    int partitions = 4;
    PushJob push = new PushJob(new PushJob.Config("cost", partitions, objects, work, 2));
    SyntheticDataset ds = SyntheticDataset.initial(keys, 100, 3);
    push.pushFull(ds, 1, 1_000);
    ExecutorService loader = Executors.newFixedThreadPool(2);
    try (RocksResources res = new RocksResources(64L << 20, 32L << 20)) {
      for (MergeMode mode : MergeMode.values()) {
        // Fill the log *before* the replica exists, then time its bootstrap (which replays the
        // whole backlog through the same merge code as live updates), minus an empty bootstrap.
        // Filling while the replica consumes would hide most of the apply time.
        double[] secs = new double[2];
        for (int run = 0; run < 2; run++) {
          int n = run == 0 ? 0 : events;
          AtomicLong clock = new AtomicLong(2_000);
          InMemoryUpdateLog log = new InMemoryUpdateLog(partitions, clock::get);
          fill(log, keys, n, 0);
          StoreEngine e =
              engine(work.resolve("e-" + mode + run), mode, partitions, res, objects, log, loader);
          long s = System.nanoTime();
          e.load(1).get(5, TimeUnit.MINUTES);
          secs[run] = (System.nanoTime() - s) / 1e9;
          e.close();
          res.awaitCleaner();
          Files2.deleteRecursively(work.resolve("e-" + mode + run));
        }
        double applySecs = secs[1] - secs[0];
        System.out.printf(
            "apply %s: %d events in %.2f s = %.0f events/s%n",
            mode, events, applySecs, events / applySecs);
        rows.add("apply,%s,%d,%.3f,%.0f".formatted(mode, events, applySecs, events / applySecs));
      }
      // Replay cost: prepare a new version when R updates have arrived since its cutoff.
      push.pushFull(ds, 2, 1_500); // cutoff 1500: everything appended at >= 2000 is "after"
      for (int backlog : new int[] {0, 10_000, 100_000, 300_000}) {
        for (MergeMode mode : new MergeMode[] {MergeMode.FULL, MergeMode.LWW}) {
          AtomicLong clock = new AtomicLong(2_000);
          InMemoryUpdateLog log = new InMemoryUpdateLog(partitions, clock::get);
          StoreEngine e =
              engine(
                  work.resolve("r-" + mode + backlog), mode, partitions, res, objects, log, loader);
          e.load(1).get(5, TimeUnit.MINUTES);
          fill(log, keys, backlog, 1);
          while (e.totalLag() > 0) {
            Thread.sleep(1);
          }
          long s = System.nanoTime();
          e.prepare(2).get(5, TimeUnit.MINUTES);
          double ms = (System.nanoTime() - s) / 1e6;
          long replayed = 0;
          for (int p = 0; p < partitions; p++) {
            PartitionReplica r = e.replica(p);
            replayed += r.lastReplayRecords();
          }
          System.out.printf(
              "prepare %s backlog=%d replayed=%d: %.1f ms%n", mode, backlog, replayed, ms);
          rows.add("prepare,%s,%d,%.3f,%d".formatted(mode, backlog, ms / 1000, replayed));
          e.close();
          Files2.deleteRecursively(work.resolve("r-" + mode + backlog));
        }
      }
    } finally {
      loader.shutdownNow();
    }
    Results.csv(out, "q2-cost.csv", "what,mode,events,seconds,extra", rows);
  }

  private static void fill(InMemoryUpdateLog log, long keys, int n, long seed) {
    SplittableRandom r = new SplittableRandom(seed);
    byte[] value = new byte[100];
    for (int i = 0; i < n; i++) {
      r.nextBytes(value);
      long k = r.nextLong(keys);
      log.append(
          UpdateEvent.newBuilder()
              .setKey(ByteString.copyFrom(SyntheticDataset.key(k)))
              .setValue(ByteString.copyFrom(value))
              .setDelete(r.nextInt(20) == 0)
              .setTs(2_000 + i)
              .setSourceId(1 + r.nextInt(3))
              .build());
    }
  }

  private static StoreEngine engine(
      Path dir,
      MergeMode mode,
      int partitions,
      RocksResources res,
      LocalObjectStore objects,
      InMemoryUpdateLog log,
      ExecutorService loader)
      throws Exception {
    StoreEngine e =
        new StoreEngine(
            new StoreEngine.Config("cost", partitions, mode, 0, false),
            res,
            dir.resolve("data"),
            objects,
            new FileCache(dir.resolve("cache"), objects),
            log,
            loader);
    for (int p = 0; p < partitions; p++) {
      e.host(p);
    }
    return e;
  }
}
