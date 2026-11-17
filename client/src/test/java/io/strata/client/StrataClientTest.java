package io.strata.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.cluster.LocalCluster;
import io.strata.common.ServeHook;
import io.strata.fault.Faults;
import io.strata.proto.v1.StoreSpec;
import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StrataClientTest {

  static final int KEYS = 2_000;
  static final StoreSpec SPEC =
      StoreSpec.newBuilder()
          .setName("feat")
          .setNumPartitions(4)
          .setReplicationFactor(3)
          .setMergeMode("FULL")
          .build();

  @TempDir static Path tmp;
  LocalCluster cluster;
  final SyntheticDataset data = SyntheticDataset.initial(KEYS, 32, 9);

  @BeforeAll
  void startCluster() throws Exception {
    cluster = new LocalCluster(tmp.resolve("c"), LocalCluster.Options.defaults(3));
    cluster.createStore(SPEC);
    cluster.pushFull(SPEC, data, 1, 1000);
    cluster.awaitServing("feat", 1, Duration.ofSeconds(30));
  }

  @AfterAll
  void stop() {
    cluster.close();
  }

  StrataClient client(ReplicaSelector.Strategy s, double hedgeBudget) {
    return new StrataClient(
        StrataClient.Options.defaults(cluster.controllerAddress())
            .withStrategy(s)
            .withHedgeBudget(hedgeBudget));
  }

  @Test
  void getAndBatchGetReturnPushedData() throws Exception {
    try (StrataClient c = client(ReplicaSelector.Strategy.C3, 0)) {
      for (long i = 0; i < 200; i++) {
        Optional<byte[]> v = c.get("feat", SyntheticDataset.key(i)).get(5, TimeUnit.SECONDS);
        assertThat(v).isPresent();
        assertThat(v.get()).isEqualTo(data.valueOf(i));
      }
      assertThat(c.get("feat", "missing".getBytes()).get(5, TimeUnit.SECONDS)).isEmpty();

      List<byte[]> keys = new ArrayList<>();
      for (long i = 0; i < 100; i++) {
        keys.add(SyntheticDataset.key(i * 17));
      }
      keys.add("missing".getBytes());
      Map<ByteString, byte[]> got = c.batchGet("feat", keys).get(5, TimeUnit.SECONDS);
      assertThat(got).hasSize(100);
      for (long i = 0; i < 100; i++) {
        assertThat(got.get(ByteString.copyFrom(SyntheticDataset.key(i * 17))))
            .isEqualTo(data.valueOf(i * 17));
      }
    }
  }

  @Test
  void c3SteersAwayFromASlowReplica() throws Exception {
    AtomicLong[] served = {new AtomicLong(), new AtomicLong(), new AtomicLong()};
    List<String> ids = cluster.liveNodeIds();
    for (int i = 0; i < 3; i++) {
      AtomicLong counter = served[i];
      ServeHook slow = Faults.slowDisk(1.0, 15, 1);
      boolean isSlow = i == 0;
      cluster
          .node(ids.get(i))
          .setServeHook(
              keys -> {
                counter.incrementAndGet();
                if (isSlow) {
                  slow.beforeServe(keys);
                }
              });
    }
    try (StrataClient c = client(ReplicaSelector.Strategy.C3, 0)) {
      runLoad(c, 1500);
      long total = Arrays.stream(served).mapToLong(AtomicLong::get).sum();
      double slowShare = (double) served[0].get() / total;
      System.out.printf("C3: slow replica got %.1f%% of requests%n", slowShare * 100);
      assertThat(slowShare).isLessThan(0.15); // round robin would give it a third
    } finally {
      ids.forEach(id -> cluster.node(id).setServeHook(null));
    }
  }

  @Test
  void hedgingCutsTheTailCausedByOneSlowReplicaAndCancelsLosers() throws Exception {
    List<String> ids = cluster.liveNodeIds();
    cluster.node(ids.get(1)).setServeHook(Faults.slowDisk(0.5, 40, 2));
    try (StrataClient noHedge = client(ReplicaSelector.Strategy.ROUND_ROBIN, 0);
        StrataClient hedged = client(ReplicaSelector.Strategy.ROUND_ROBIN, 0.3)) {
      long p99Plain = runLoad(noHedge, 1500);
      runLoad(hedged, 300); // warm up the adaptive hedge delay
      long p99Hedged = runLoad(hedged, 1500);
      System.out.printf(
          "p99 no hedge=%.1fms hedged=%.1fms hedges=%d wins=%d cancelled=%d%n",
          p99Plain / 1e6,
          p99Hedged / 1e6,
          hedged.stats().hedges.sum(),
          hedged.stats().hedgeWins.sum(),
          hedged.stats().cancelled.sum());
      assertThat(p99Hedged).isLessThan(p99Plain / 2);
      assertThat(hedged.stats().hedges.sum())
          .isLessThanOrEqualTo((long) (hedged.stats().primaries.sum() * 0.3) + 10);
      assertThat(hedged.stats().cancelled.sum()).isPositive();
    } finally {
      cluster.node(ids.get(1)).setServeHook(null);
    }
  }

  @Test
  void failsOverWhenANodeDies() throws Exception {
    try (LocalCluster c = new LocalCluster(tmp.resolve("f"), LocalCluster.Options.defaults(3))) {
      c.createStore(SPEC);
      c.pushFull(SPEC, data, 1, 1000);
      c.awaitServing("feat", 1, Duration.ofSeconds(30));
      try (StrataClient client =
          new StrataClient(
              StrataClient.Options.defaults(c.controllerAddress())
                  .withStrategy(ReplicaSelector.Strategy.ROUND_ROBIN)
                  .withHedgeBudget(0))) {
        c.kill("n2"); // the client still thinks n2 is ready until its next view refresh
        int errors = 0;
        for (long i = 0; i < 300; i++) {
          try {
            assertThat(client.get("feat", SyntheticDataset.key(i)).get(5, TimeUnit.SECONDS))
                .isPresent();
          } catch (Exception e) {
            errors++;
          }
        }
        assertThat(errors).isZero();
        assertThat(client.stats().failovers.sum()).isPositive();
      }
    }
  }

  /** Sends n reads, at most 16 in flight; returns p99 latency in ns. */
  long runLoad(StrataClient c, int n) throws Exception {
    long[] lat = new long[n];
    List<CompletableFuture<?>> window = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      int idx = i;
      long s = System.nanoTime();
      window.add(
          c.get("feat", SyntheticDataset.key(i % KEYS))
              .thenRun(() -> lat[idx] = System.nanoTime() - s));
      if (window.size() == 16) {
        window.remove(0).get(10, TimeUnit.SECONDS);
      }
    }
    for (CompletableFuture<?> f : window) {
      f.get(10, TimeUnit.SECONDS);
    }
    Arrays.sort(lat);
    return lat[(int) (n * 0.99)];
  }
}
