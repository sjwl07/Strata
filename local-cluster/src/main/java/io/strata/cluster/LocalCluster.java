package io.strata.cluster;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.strata.common.InMemoryUpdateLog;
import io.strata.common.LocalObjectStore;
import io.strata.controller.ControllerServer;
import io.strata.node.ClusterAgent;
import io.strata.node.StorageNode;
import io.strata.node.engine.Files2;
import io.strata.proto.v1.ClusterView;
import io.strata.proto.v1.ClusterViewRequest;
import io.strata.proto.v1.ControllerGrpc;
import io.strata.proto.v1.PushStatus;
import io.strata.proto.v1.PushStatusRequest;
import io.strata.proto.v1.StartPushRequest;
import io.strata.proto.v1.StoreSpec;
import io.strata.proto.v1.StoreView;
import io.strata.proto.v1.VersionManifest;
import io.strata.push.Dataset;
import io.strata.push.PushJob;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * A controller and N storage nodes in one JVM. Each node has its own data directory and its own
 * Netty gRPC server on a loopback port, so requests really cross the network stack. Update logs are
 * in-memory and shared, standing in for a Kafka cluster (ADR 0001).
 *
 * <p>{@link #kill} simulates {@code kill -9}: the node's server and engines stop abruptly and its
 * disk state is discarded. {@link #restart} brings a node with the same id back empty, so it must
 * bootstrap from the object store plus the log.
 */
public final class LocalCluster implements AutoCloseable {

  public record Options(int nodes, int serveThreads, Duration heartbeat, long nodeTimeoutMs) {
    public static Options defaults(int nodes) {
      return new Options(nodes, 4, Duration.ofMillis(100), 1000);
    }
  }

  private final Path root;
  private final Options options;
  private final LocalObjectStore objects;
  private final ControllerServer controller;
  private final ManagedChannel controllerChannel;
  private final ControllerGrpc.ControllerBlockingStub stub;
  private final Map<String, InMemoryUpdateLog> logs = new ConcurrentHashMap<>();
  private final Map<String, Running> nodes = new ConcurrentHashMap<>();
  private final Map<String, Integer> ports = new ConcurrentHashMap<>();
  private int restarts;

  private record Running(StorageNode node, ClusterAgent agent) {}

  public LocalCluster(Path root, Options options) throws IOException {
    Files2.deleteRecursively(root);
    this.root = root;
    this.options = options;
    this.objects = new LocalObjectStore(root.resolve("objects"));
    this.controller = new ControllerServer("127.0.0.1", 0, options.nodeTimeoutMs());
    this.controllerChannel =
        NettyChannelBuilder.forTarget(controller.address()).usePlaintext().build();
    this.stub = ControllerGrpc.newBlockingStub(controllerChannel);
    for (int i = 1; i <= options.nodes(); i++) {
      start("n" + i);
    }
  }

  public LocalObjectStore objects() {
    return objects;
  }

  public String controllerAddress() {
    return controller.address();
  }

  public ControllerServer controller() {
    return controller;
  }

  /** The shared update log of a store (created with the store). */
  public InMemoryUpdateLog log(String store) {
    return logs.get(store);
  }

  public void createStore(StoreSpec spec) {
    logs.computeIfAbsent(spec.getName(), n -> new InMemoryUpdateLog((int) spec.getNumPartitions()));
    stub.createStore(spec);
  }

  /** Builds a version with the push job and announces it; returns once it serves everywhere. */
  public VersionManifest pushFull(StoreSpec spec, Dataset data, long version, long watermark)
      throws Exception {
    VersionManifest m = pushJob(spec).pushFull(data, version, watermark);
    startPush(spec.getName(), version);
    return m;
  }

  public PushJob pushJob(StoreSpec spec) {
    return new PushJob(
        new PushJob.Config(
            spec.getName(), (int) spec.getNumPartitions(), objects, root.resolve("push"), 2));
  }

  public PushStatus startPush(String store, long version) {
    return stub.startPush(
        StartPushRequest.newBuilder().setStore(store).setVersion(version).build());
  }

  public PushStatus pushStatus(String store, long version) {
    return stub.getPushStatus(
        PushStatusRequest.newBuilder().setStore(store).setVersion(version).build());
  }

  public ClusterView view() {
    return stub.getClusterView(ClusterViewRequest.getDefaultInstance());
  }

  /** Waits until {@code version} is the serving version and every assigned replica serves it. */
  public void awaitServing(String store, long version, Duration timeout) throws Exception {
    await(
        () -> {
          for (StoreView s : view().getStoresList()) {
            if (s.getSpec().getName().equals(store)) {
              if (s.getServingVersion() != version) {
                return false;
              }
              return s.getPartitionsList().stream()
                  .allMatch(
                      p ->
                          p.getReplicasCount() == s.getSpec().getReplicationFactor()
                              && p.getReplicasList().stream().allMatch(r -> r.getReady()));
            }
          }
          return false;
        },
        timeout,
        "store " + store + " serving v" + version);
  }

  /** Waits until no hosted replica on any live node has unapplied log records. */
  public void awaitCaughtUp(Duration timeout) throws Exception {
    await(
        () ->
            nodes.values().stream()
                .allMatch(
                    r -> r.node().stores().values().stream().allMatch(e -> e.totalLag() == 0)),
        timeout,
        "replicas caught up");
  }

  public static void await(BooleanSupplier cond, Duration timeout, String what) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("timed out waiting for " + what);
      }
      Thread.sleep(20);
    }
  }

  public synchronized void start(String nodeId) throws IOException {
    if (nodes.containsKey(nodeId)) {
      return;
    }
    Path dir = root.resolve("nodes").resolve(nodeId + "-" + (restarts++));
    StorageNode.Config cfg =
        new StorageNode.Config(
                nodeId, "127.0.0.1", ports.getOrDefault(nodeId, 0), dir, 4, 32L << 20, 16L << 20)
            .withServeThreads(options.serveThreads());
    StorageNode node = new StorageNode(cfg, objects).start();
    ports.put(nodeId, node.port()); // a restarted node comes back on the same address
    ClusterAgent agent =
        new ClusterAgent(
                node, controllerChannel, spec -> logs.get(spec.getName()), options.heartbeat())
            .start();
    nodes.put(nodeId, new Running(node, agent));
  }

  /** Abrupt stop and loss of local disk state (simulated kill -9 + lost disk). */
  public synchronized void kill(String nodeId) throws IOException {
    Running r = nodes.remove(nodeId);
    if (r != null) {
      r.agent().close();
      r.node().close();
    }
  }

  public void restart(String nodeId) throws IOException {
    kill(nodeId);
    start(nodeId);
  }

  public StorageNode node(String nodeId) {
    Running r = nodes.get(nodeId);
    return r == null ? null : r.node();
  }

  public List<String> liveNodeIds() {
    return new ArrayList<>(nodes.keySet()).stream().sorted().toList();
  }

  @Override
  public void close() {
    for (String id : List.copyOf(nodes.keySet())) {
      try {
        kill(id);
      } catch (IOException ignored) {
        // shutting down
      }
    }
    controllerChannel.shutdownNow();
    try {
      controllerChannel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    controller.close();
    logs.values().forEach(InMemoryUpdateLog::close);
    try {
      Files2.deleteRecursively(root);
    } catch (IOException ignored) {
      // best effort
    }
  }
}
