package io.strata.bench;

import com.google.protobuf.ByteString;
import io.strata.client.StrataClient;
import io.strata.cluster.LocalCluster;
import io.strata.proto.v1.StoreSpec;
import io.strata.proto.v1.UpdateEvent;
import io.strata.push.MapDataset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * The quickstart: the "Ravi" story from docs/CONCEPTS.md on a real 3-node cluster.
 *
 * <pre>./gradlew :bench:installDist && bench/build/install/bench/bin/bench demo</pre>
 */
final class Demo {
  private Demo() {}

  static void run(Args a) throws Exception {
    Path work = Path.of(a.s("work", "build/demo")).toAbsolutePath();
    StoreSpec spec =
        StoreSpec.newBuilder()
            .setName("phonebook")
            .setNumPartitions(4)
            .setReplicationFactor(2)
            .setMergeMode(a.s("mode", "FULL"))
            .setMaxClockSkewMs(500)
            .build();
    say("Starting a controller and 3 storage nodes (each a real gRPC server on loopback)...");
    try (LocalCluster c = new LocalCluster(work, LocalCluster.Options.defaults(3))) {
      c.createStore(spec);
      say("Controller at " + c.controllerAddress() + ", nodes " + c.liveNodeIds());

      long t0 = System.currentTimeMillis();
      say("1. Nightly batch job publishes edition v1: ravi=111, asha=555");
      c.pushFull(spec, rows("ravi", "111", "asha", "555"), 1, t0 - 60_000);
      c.awaitServing("phonebook", 1, Duration.ofSeconds(30));

      try (StrataClient client =
          new StrataClient(StrataClient.Options.defaults(c.controllerAddress()))) {
        show(client, "ravi");

        long cutoff = System.currentTimeMillis();
        Thread.sleep(700); // stay clear of the 500 ms clock-skew bound
        say("2. At 03:00 a sticky note arrives on the stream: ravi -> 222");
        c.log("phonebook")
            .append(
                UpdateEvent.newBuilder()
                    .setKey(ByteString.copyFromUtf8("ravi"))
                    .setValue(ByteString.copyFromUtf8("222"))
                    .setTs(System.currentTimeMillis())
                    .setSourceId(1)
                    .build());
        Thread.sleep(300);
        show(client, "ravi");

        say("3. Edition v2 arrives. It was photographed at the cutoff, BEFORE the note: ravi=111");
        c.pushFull(spec, rows("ravi", "111", "asha", "556"), 2, cutoff);
        c.awaitServing("phonebook", 2, Duration.ofSeconds(30));
        client.refreshView();
        Optional<byte[]> v = show(client, "ravi");
        boolean kept = v.isPresent() && new String(v.get(), StandardCharsets.UTF_8).equals("222");
        say(
            kept
                ? "   -> the note survived the swap (replayed onto v2 before it went live)"
                : "   -> the note was LOST by the swap (mode=" + spec.getMergeMode() + ")");
        show(client, "asha");

        String victim = c.liveNodeIds().get(0);
        say("4. kill -9 " + victim + " (its disk is gone too). Reads keep working:");
        c.kill(victim);
        show(client, "ravi");
        show(client, "asha");
        say("   client stats: failovers=" + client.stats().failovers.sum());

        say("5. Bring " + victim + " back empty. It bootstraps from the SSTs plus a log replay.");
        long s = System.nanoTime();
        c.start(victim);
        // Rendezvous placement hands its partitions back; wait until it serves them itself.
        LocalCluster.await(
            () ->
                c.view().getStores(0).getPartitionsList().stream()
                    .flatMap(p -> p.getReplicasList().stream())
                    .anyMatch(r -> r.getNodeId().equals(victim) && r.getReady()),
            Duration.ofSeconds(30),
            victim + " serving");
        c.awaitServing("phonebook", 2, Duration.ofSeconds(30));
        say(
            "   %s is serving again after %d ms"
                .formatted(victim, (System.nanoTime() - s) / 1_000_000));
      }
    }
    say("Done. Try again with --mode=NAIVE to watch step 3 lose the update.");
  }

  private static MapDataset rows(String... kv) {
    Map<String, byte[]> m = new java.util.HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1].getBytes(StandardCharsets.UTF_8));
    }
    return new MapDataset(m);
  }

  private static Optional<byte[]> show(StrataClient client, String key) throws Exception {
    Optional<byte[]> v = client.get("phonebook", key.getBytes(StandardCharsets.UTF_8)).get();
    say(
        "   get("
            + key
            + ") = "
            + v.map(b -> new String(b, StandardCharsets.UTF_8)).orElse("<none>"));
    return v;
  }

  private static void say(String s) {
    System.out.println(s);
  }
}
