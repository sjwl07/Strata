package io.strata.bench;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.strata.common.LocalObjectStore;
import io.strata.node.StorageNode;
import io.strata.node.engine.Files2;
import io.strata.node.engine.StoreEngine;
import io.strata.proto.v1.GetRequest;
import io.strata.proto.v1.StorageNodeGrpc;
import io.strata.push.PushJob;
import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;

/**
 * Phase 1 exit experiment: single node, one store. Measures point-read latency at a fixed request
 * rate (uniform and Zipfian keys), then swaps to a new full version while under load and records
 * per-second latency around the swap.
 */
final class Phase1Baseline {
  private Phase1Baseline() {}

  static void run(Args a) throws Exception {
    long keys = a.l("keys", 1_000_000);
    int valueSize = a.i("valueSize", 100);
    int partitions = a.i("partitions", 8);
    double rate = a.d("rate", 5_000);
    double seconds = a.d("seconds", 20);
    Path work = Path.of(a.s("work", "build/experiments/phase1")).toAbsolutePath();
    Path out = Path.of(a.s("out", "../docs/experiments/data")).toAbsolutePath();
    Files2.deleteRecursively(work);

    LocalObjectStore objects = new LocalObjectStore(work.resolve("objects"));
    PushJob push =
        new PushJob(new PushJob.Config("bench", partitions, objects, work.resolve("push"), 2));
    SyntheticDataset v1 = SyntheticDataset.initial(keys, valueSize, 42);
    SyntheticDataset v2 = v1.next(1.0, 0);
    long t = System.nanoTime();
    push.pushFull(v1, 1, 1_000);
    long push1Ms = (System.nanoTime() - t) / 1_000_000;
    push.pushFull(v2, 2, 2_000);
    System.out.printf("push: %d keys x %dB in %d ms%n", keys, valueSize, push1Ms);

    try (StorageNode node =
        new StorageNode(StorageNode.Config.local("n1", work.resolve("node")), objects).start()) {
      StoreEngine store = node.addStore(new StoreEngine.Config("bench", partitions), null);
      for (int p = 0; p < partitions; p++) {
        store.host(p);
      }
      t = System.nanoTime();
      store.load(1).get(10, TimeUnit.MINUTES);
      long loadMs = (System.nanoTime() - t) / 1_000_000;
      System.out.printf("ingest v1: %d ms%n", loadMs);

      ManagedChannel ch = NettyChannelBuilder.forTarget(node.address()).usePlaintext().build();
      StorageNodeGrpc.StorageNodeFutureStub stub = StorageNodeGrpc.newFutureStub(ch);
      ZipfianGenerator zipf = new ZipfianGenerator(keys, 0.99, 1, true);
      java.util.SplittableRandom uniform = new java.util.SplittableRandom(2);
      AtomicLong notFound = new AtomicLong();

      List<String> rows = new ArrayList<>();
      List<String> runs = new ArrayList<>(List.of("warmup@" + rate));
      for (String rs : a.s("rates", String.valueOf((long) rate)).split(",")) {
        runs.add("uniform@" + rs);
        runs.add("zipfian@" + rs);
      }
      for (String run : runs) {
        String dist = run.substring(0, run.indexOf('@'));
        double runRate = Double.parseDouble(run.substring(run.indexOf('@') + 1));
        OpenLoopLoad.Result r =
            OpenLoopLoad.run(
                runRate,
                dist.equals("warmup") ? 5 : seconds,
                i -> {
                  long k = dist.equals("uniform") ? uniform.nextLong(keys) : zipf.next();
                  return Grpc.toCf(
                          stub.get(
                              GetRequest.newBuilder()
                                  .setKey(ByteString.copyFrom(SyntheticDataset.key(k)))
                                  .build()))
                      .thenAccept(
                          resp -> {
                            if (!resp.getFound()) {
                              notFound.incrementAndGet();
                            }
                          });
                });
        System.out.printf(
            "%-8s rate=%.0f/s errors=%d %s%n",
            dist, runRate, r.errors(), OpenLoopLoad.summary(r.total()));
        if (r.errors() > 0 || notFound.get() > 0) {
          throw new IllegalStateException(
              "errors=" + r.errors() + " notFound=" + notFound.get() + ": results invalid");
        }
        if (!dist.equals("warmup")) {
          rows.add(row(dist, runRate, r.total(), r.errors()));
        }
      }
      Results.csv(
          out,
          "phase1-baseline.csv",
          "keys,dist,rate,p50_ms,p99_ms,p999_ms,max_ms,errors",
          rows.stream().map(s -> keys + "," + s).toList());

      // Swap under load: start a 30 s run, trigger prepare+swap of v2 at ~10 s.
      long[] prepMs = new long[1];
      long[] swapAtSec = new long[1];
      Thread swapper =
          new Thread(
              () -> {
                try {
                  Thread.sleep(10_000);
                  swapAtSec[0] = 10;
                  long s = System.nanoTime();
                  store.prepare(2).get(10, TimeUnit.MINUTES);
                  prepMs[0] = (System.nanoTime() - s) / 1_000_000;
                  store.swap(2).get(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      swapper.start();
      OpenLoopLoad.Result r =
          OpenLoopLoad.run(
              rate,
              30,
              i ->
                  Grpc.toCf(
                      stub.get(
                          GetRequest.newBuilder()
                              .setKey(ByteString.copyFrom(SyntheticDataset.key(zipf.next())))
                              .build())));
      swapper.join();
      System.out.printf(
          "swap-under-load: prepare(v2)=%d ms; %s%n", prepMs[0], OpenLoopLoad.summary(r.total()));
      List<String> timeline = new ArrayList<>();
      for (int s = 0; s < r.perSecond().size(); s++) {
        Histogram h = r.perSecond().get(s);
        timeline.add(
            "%d,%.3f,%.3f,%.3f"
                .formatted(
                    s, OpenLoopLoad.ms(h, 50), OpenLoopLoad.ms(h, 99), h.getMaxValue() / 1e6));
      }
      Results.csv(out, "phase1-swap-timeline.csv", "second,p50_ms,p99_ms,max_ms", timeline);
      System.out.printf("prepare started at t=%ds and took %d ms%n", swapAtSec[0], prepMs[0]);
      ch.shutdownNow();
    }
    Files2.deleteRecursively(work);
  }

  private static String row(String dist, double rate, Histogram h, long errors) {
    return "%s,%.0f,%.3f,%.3f,%.3f,%.3f,%d"
        .formatted(
            dist,
            rate,
            OpenLoopLoad.ms(h, 50),
            OpenLoopLoad.ms(h, 99),
            OpenLoopLoad.ms(h, 99.9),
            h.getMaxValue() / 1e6,
            errors);
  }
}
