package io.strata.node;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.strata.common.LocalObjectStore;
import io.strata.kafka.KafkaUpdateLog;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Runs a storage node that joins a cluster.
 *
 * <pre>
 * storage-node --node-id=n1 --port=7101 --data-dir=/var/strata/n1 --objects=/shared/objects
 *              --controller=localhost:7000 [--kafka=localhost:9092] [--host=127.0.0.1]
 *              [--serve-threads=8] [--heartbeat-ms=500]
 * </pre>
 *
 * Without {@code --kafka} stores are batch-only (no stream updates).
 */
public final class StorageNodeMain {

  private StorageNodeMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> o = new HashMap<>();
    for (String a : args) {
      o.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
    }
    String nodeId = o.getOrDefault("node-id", "node-" + ProcessHandle.current().pid());
    StorageNode.Config cfg =
        new StorageNode.Config(
            nodeId,
            o.getOrDefault("host", "127.0.0.1"),
            Integer.parseInt(o.getOrDefault("port", "7101")),
            Path.of(o.getOrDefault("data-dir", "/tmp/strata-" + nodeId)),
            Integer.parseInt(o.getOrDefault("serve-threads", "8")),
            Long.parseLong(o.getOrDefault("block-cache-mb", "256")) << 20,
            Long.parseLong(o.getOrDefault("write-buffer-mb", "128")) << 20);
    LocalObjectStore objects = new LocalObjectStore(Path.of(required(o, "objects")));
    StorageNode node = new StorageNode(cfg, objects).start();
    String kafka = o.get("kafka");
    ClusterAgent.LogFactory logs =
        spec -> {
          if (kafka == null) {
            return null;
          }
          try {
            return new KafkaUpdateLog(
                kafka, "strata-" + spec.getName(), (int) spec.getNumPartitions(), (short) 1);
          } catch (Exception e) {
            throw new IllegalStateException("cannot connect to Kafka at " + kafka, e);
          }
        };
    ManagedChannel controller =
        NettyChannelBuilder.forTarget(required(o, "controller")).usePlaintext().build();
    ClusterAgent agent =
        new ClusterAgent(
                node,
                controller,
                logs,
                Duration.ofMillis(Long.parseLong(o.getOrDefault("heartbeat-ms", "500"))))
            .start();
    System.out.println("storage node " + nodeId + " serving on " + node.address());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  agent.close();
                  node.close();
                  controller.shutdownNow();
                }));
    Thread.currentThread().join();
  }

  private static String required(Map<String, String> o, String k) {
    String v = o.get(k);
    if (v == null) {
      throw new IllegalArgumentException("missing --" + k + "=...");
    }
    return v;
  }
}
