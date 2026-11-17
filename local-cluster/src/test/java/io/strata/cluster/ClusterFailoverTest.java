package io.strata.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.common.StoredValue;
import io.strata.node.StorageNode;
import io.strata.node.engine.Lookup;
import io.strata.proto.v1.PartitionView;
import io.strata.proto.v1.ReplicaView;
import io.strata.proto.v1.StoreSpec;
import io.strata.proto.v1.StoreView;
import io.strata.proto.v1.UpdateEvent;
import io.strata.push.MapDataset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 4 exit criterion: a 5-node cluster keeps serving and converges to exactly the right data
 * when nodes are killed (losing their disks) during a push and during streaming, then rejoin.
 */
class ClusterFailoverTest {

  @TempDir Path tmp;

  static final int KEYS = 400;

  record Ev(int id, int key, long ts, boolean delete) {
    StoredValue stored() {
      return delete ? StoredValue.tombstone(ts, 1) : StoredValue.live(ts, 1, value());
    }

    byte[] value() {
      return ("e" + id).getBytes(StandardCharsets.US_ASCII);
    }
  }

  static String key(int k) {
    return "user-%04d".formatted(k);
  }

  @Test
  void survivesNodeLossDuringPushAndStreamAndConverges() throws Exception {
    StoreSpec spec =
        StoreSpec.newBuilder()
            .setName("pymk")
            .setNumPartitions(6)
            .setReplicationFactor(3)
            .setMergeMode("FULL")
            .setMaxClockSkewMs(200)
            .build();
    try (LocalCluster c = new LocalCluster(tmp.resolve("c"), LocalCluster.Options.defaults(5))) {
      c.createStore(spec);
      List<Ev> events = new ArrayList<>();
      c.pushFull(spec, new MapDataset(Map.of()), 1, System.currentTimeMillis() - 1000);
      c.awaitServing("pymk", 1, Duration.ofSeconds(30));

      AtomicBoolean stop = new AtomicBoolean();
      Thread producer =
          new Thread(
              () -> {
                SplittableRandom r = new SplittableRandom(42);
                int id = 0;
                while (!stop.get()) {
                  Ev e =
                      new Ev(id++, r.nextInt(KEYS), System.currentTimeMillis(), r.nextInt(10) == 0);
                  synchronized (events) {
                    events.add(e);
                  }
                  UpdateEvent.Builder b =
                      UpdateEvent.newBuilder()
                          .setKey(ByteString.copyFromUtf8(key(e.key())))
                          .setTs(e.ts())
                          .setSourceId(1)
                          .setDelete(e.delete());
                  if (!e.delete()) {
                    b.setValue(ByteString.copyFrom(e.value()));
                  }
                  c.log("pymk").append(b.build());
                  try {
                    Thread.sleep(1);
                  } catch (InterruptedException ex) {
                    return;
                  }
                }
              });
      producer.start();
      Thread.sleep(1000);

      // Push v2 with a cutoff in the past; kill a node while replicas prepare it.
      long w2 = System.currentTimeMillis() - 300;
      c.pushJob(spec).pushFull(snapshot(events, w2), 2, w2);
      c.startPush("pymk", 2);
      c.kill("n2");
      c.awaitServing("pymk", 2, Duration.ofSeconds(30));
      Thread.sleep(500);

      // Lose another node mid-stream, bring both back empty.
      c.kill("n4");
      Thread.sleep(1500); // past the node timeout: its partitions move
      c.start("n2");
      c.start("n4");

      // Another push while the rejoined nodes are bootstrapping.
      long w3 = System.currentTimeMillis() - 300;
      c.pushJob(spec).pushFull(snapshot(events, w3), 3, w3);
      c.startPush("pymk", 3);
      c.awaitServing("pymk", 3, Duration.ofSeconds(30));
      Thread.sleep(500);
      stop.set(true);
      producer.join();
      c.awaitServing("pymk", 3, Duration.ofSeconds(30));
      c.awaitCaughtUp(Duration.ofSeconds(30));

      // Every replica of every partition must hold exactly the newest value of every key.
      Map<Integer, Ev> truth = new HashMap<>();
      for (Ev e : events) {
        truth.merge(e.key(), e, (a, b) -> a.stored().compareTo(b.stored()) >= 0 ? a : b);
      }
      StoreView view = c.view().getStores(0);
      int checked = 0;
      List<String> wrong = new ArrayList<>();
      for (int k = 0; k < KEYS; k++) {
        byte[] kb = key(k).getBytes(StandardCharsets.UTF_8);
        int p = io.strata.common.Partitioner.partition(kb, 6);
        PartitionView pv = view.getPartitions(p);
        assertThat(pv.getReplicasCount()).isEqualTo(3);
        for (ReplicaView rv : pv.getReplicasList()) {
          StorageNode n = c.node(rv.getNodeId());
          Lookup l = n.store("pymk").get(kb);
          Ev t = truth.get(k);
          boolean ok =
              (t == null || t.delete())
                  ? !l.found()
                  : l.found() && Arrays.equals(l.value(), t.value());
          if (!ok) {
            wrong.add(key(k) + "@" + rv.getNodeId());
          }
          checked++;
        }
      }
      System.out.printf(
          "events=%d, replica reads checked=%d, wrong=%d%n", events.size(), checked, wrong.size());
      assertThat(events.size()).isGreaterThan(1000);
      assertThat(wrong).isEmpty();
    }
  }

  private static MapDataset snapshot(List<Ev> events, long w) {
    Map<Integer, Ev> best = new HashMap<>();
    synchronized (events) {
      for (Ev e : events) {
        if (e.ts() <= w) {
          best.merge(e.key(), e, (a, b) -> a.stored().compareTo(b.stored()) >= 0 ? a : b);
        }
      }
    }
    Map<String, byte[]> rows = new HashMap<>();
    best.forEach(
        (k, e) -> {
          if (!e.delete()) {
            rows.put(key(k), e.value());
          }
        });
    return new MapDataset(rows);
  }
}
