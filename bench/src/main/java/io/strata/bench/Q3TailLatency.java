package io.strata.bench;

import io.strata.client.ReplicaSelector.Strategy;
import io.strata.client.StrataClient;
import io.strata.cluster.LocalCluster;
import io.strata.common.ServeHook;
import io.strata.fault.Faults;
import io.strata.proto.v1.StoreSpec;
import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Q3: tail latency under realistic slowness, per replica-selection and hedging strategy.
 *
 * <p>Cluster: 6 in-process nodes (real gRPC over loopback), 12 partitions, RF 3, few serving
 * threads per node so a slow node really queues. Each cell: fresh client, open-loop Zipfian reads
 * at a fixed rate, latency measured from the intended send time.
 */
final class Q3TailLatency {
  private Q3TailLatency() {}

  record Variant(String name, Strategy strategy, double hedgeBudget) {}

  static void run(Args a) throws Exception {
    long keys = a.l("keys", 100_000);
    double rate = a.d("rate", 2_000);
    double seconds = a.d("seconds", 15);
    int nodes = a.i("nodes", 6);
    Path work = Path.of(a.s("work", "build/experiments/q3")).toAbsolutePath();
    Path out = Path.of(a.s("out", "../docs/experiments/data")).toAbsolutePath();

    List<Variant> variants =
        List.of(
            new Variant("round-robin", Strategy.ROUND_ROBIN, 0),
            new Variant("least-outstanding", Strategy.LEAST_OUTSTANDING, 0),
            new Variant("c3", Strategy.C3, 0),
            new Variant("round-robin+hedge5%", Strategy.ROUND_ROBIN, 0.05),
            new Variant("c3+hedge5%", Strategy.C3, 0.05),
            new Variant("round-robin+hedge-unbounded", Strategy.ROUND_ROBIN, 1.0));
    List<String> wantedVariants = List.of(a.s("variants", "all").split(","));
    List<String> wantedFaults = List.of(a.s("faults", "all").split(","));

    StoreSpec spec =
        StoreSpec.newBuilder()
            .setName("q3")
            .setNumPartitions(12)
            .setReplicationFactor(3)
            .setMergeMode("FULL")
            .build();
    List<String> rows = new ArrayList<>();
    LocalCluster.Options opts =
        new LocalCluster.Options(nodes, a.i("serveThreads", 2), Duration.ofMillis(100), 1500);
    try (LocalCluster c = new LocalCluster(work, opts)) {
      c.createStore(spec);
      c.pushFull(spec, SyntheticDataset.initial(keys, 100, 5), 1, 1000);
      c.awaitServing("q3", 1, Duration.ofMinutes(2));
      List<String> ids = c.liveNodeIds();

      Map<String, Map<String, ServeHook>> faults = new LinkedHashMap<>();
      faults.put("none", Map.of());
      faults.put("slow-node", Map.of(ids.get(0), Faults.slowDisk(0.2, 10, 1)));
      faults.put(
          "gc-pauses", Map.of(ids.get(0), Faults.pauses(1000, 100, System::currentTimeMillis)));
      faults.put(
          "noisy-neighbours",
          Map.of(
              ids.get(0),
              Faults.noisyNeighbour(2000, 300, 10, 7, System::currentTimeMillis),
              ids.get(1),
              Faults.noisyNeighbour(2000, 300, 10, 8, System::currentTimeMillis)));

      ZipfianGenerator zipfBase = new ZipfianGenerator(keys, 0.99, 1, true);
      for (var fault : faults.entrySet()) {
        if (!wantedFaults.contains("all") && !wantedFaults.contains(fault.getKey())) {
          continue;
        }
        for (Variant v : variants) {
          if (!wantedVariants.contains("all") && !wantedVariants.contains(v.name())) {
            continue;
          }
          ids.forEach(id -> c.node(id).setServeHook(fault.getValue().get(id)));
          ZipfianGenerator zipf = zipfBase.withSeed(v.name().hashCode());
          try (StrataClient client =
              new StrataClient(
                  StrataClient.Options.defaults(c.controllerAddress())
                      .withStrategy(v.strategy())
                      .withHedgeBudget(v.hedgeBudget()))) {
            OpenLoopLoad.run(rate, 3, i -> client.get("q3", SyntheticDataset.key(zipf.next())));
            long p0 = client.stats().primaries.sum();
            long h0 = client.stats().hedges.sum();
            long w0 = client.stats().hedgeWins.sum();
            OpenLoopLoad.Result r =
                OpenLoopLoad.run(
                    rate, seconds, i -> client.get("q3", SyntheticDataset.key(zipf.next())));
            long prim = client.stats().primaries.sum() - p0;
            long hedges = client.stats().hedges.sum() - h0;
            long wins = client.stats().hedgeWins.sum() - w0;
            String row =
                "%s,%s,%.0f,%.3f,%.3f,%.3f,%.3f,%.2f,%d,%d"
                    .formatted(
                        fault.getKey(),
                        v.name(),
                        rate,
                        OpenLoopLoad.ms(r.total(), 50),
                        OpenLoopLoad.ms(r.total(), 99),
                        OpenLoopLoad.ms(r.total(), 99.9),
                        r.total().getMaxValue() / 1e6,
                        prim == 0 ? 0 : 100.0 * hedges / prim,
                        wins,
                        r.errors());
            rows.add(row);
            System.out.println(row);
          }
          ids.forEach(id -> c.node(id).setServeHook(null));
          Thread.sleep(500);
        }
      }
    }
    Results.csv(
        out,
        a.s("csv", "q3-tail-latency.csv"),
        "fault,strategy,rate,p50_ms,p99_ms,p999_ms,max_ms,extra_load_pct,hedge_wins,errors",
        rows);
  }
}
