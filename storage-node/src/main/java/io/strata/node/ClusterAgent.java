package io.strata.node;

import io.grpc.ManagedChannel;
import io.strata.common.UpdateLog;
import io.strata.node.engine.MergeMode;
import io.strata.node.engine.PartitionReplica;
import io.strata.node.engine.StoreEngine;
import io.strata.proto.v1.ControllerGrpc;
import io.strata.proto.v1.HeartbeatRequest;
import io.strata.proto.v1.HeartbeatResponse;
import io.strata.proto.v1.ReplicaStatus;
import io.strata.proto.v1.StoreAssignment;
import io.strata.proto.v1.StoreSpec;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Connects a {@link StorageNode} to the controller. Every tick it sends a heartbeat with the state
 * of each hosted partition and <b>reconciles</b> against the answer: host or drop partitions,
 * prepare the serving or target version, swap when the serving version is prepared.
 *
 * <p>Level-triggered: each response is the complete desired state, so the agent never needs to
 * remember what it was told before. A lost heartbeat or a restarted node is repaired by the next
 * tick (the Kubernetes controller pattern).
 */
public final class ClusterAgent implements AutoCloseable {

  /** Supplies the update log for a store (null = batch-only store). */
  @FunctionalInterface
  public interface LogFactory {
    UpdateLog logFor(StoreSpec spec);
  }

  private final StorageNode node;
  private final ControllerGrpc.ControllerBlockingStub controller;
  private final LogFactory logs;
  private final ScheduledExecutorService ticker;
  private final Duration interval;
  // (store/partition) -> version being prepared; prevents starting the same work twice.
  private final Map<String, Long> inflight = new ConcurrentHashMap<>();
  private volatile long lastSuccessfulBeat;
  private volatile Throwable lastError;

  public ClusterAgent(
      StorageNode node, ManagedChannel controllerChannel, LogFactory logs, Duration interval) {
    this.node = node;
    this.controller = ControllerGrpc.newBlockingStub(controllerChannel);
    this.logs = logs;
    this.interval = interval;
    this.ticker =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, node.nodeId() + "-agent");
              t.setDaemon(true);
              return t;
            });
  }

  public ClusterAgent start() {
    ticker.scheduleWithFixedDelay(this::tickSafely, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    return this;
  }

  private void tickSafely() {
    try {
      tick();
      lastSuccessfulBeat = System.currentTimeMillis();
    } catch (Throwable t) {
      lastError = t; // controller unreachable etc.: keep serving what we have, retry next tick
    }
  }

  public Throwable lastError() {
    return lastError;
  }

  public long lastSuccessfulBeat() {
    return lastSuccessfulBeat;
  }

  void tick() {
    HeartbeatRequest.Builder req =
        HeartbeatRequest.newBuilder().setNodeId(node.nodeId()).setAddress(node.address());
    node.stores()
        .forEach(
            (name, engine) -> {
              for (int p : engine.hosted()) {
                PartitionReplica r = engine.replica(p);
                if (r == null) {
                  continue;
                }
                req.addReplicas(
                    ReplicaStatus.newBuilder()
                        .setStore(name)
                        .setPartition(p)
                        .setCurrentVersion(Math.max(0, r.currentVersion()))
                        .setPreparedVersion(Math.max(0, r.preparedVersion()))
                        .setLag(r.lag()));
              }
            });
    HeartbeatResponse resp =
        controller
            .withDeadlineAfter(Math.max(1000, interval.toMillis() * 5), TimeUnit.MILLISECONDS)
            .heartbeat(req.build());
    for (StoreAssignment a : resp.getStoresList()) {
      reconcile(a);
    }
  }

  private void reconcile(StoreAssignment a) {
    StoreSpec spec = a.getSpec();
    StoreEngine engine = node.store(spec.getName());
    if (engine == null) {
      if (a.getPartitionsCount() == 0) {
        return;
      }
      engine =
          node.addStore(
              new StoreEngine.Config(
                  spec.getName(),
                  spec.getNumPartitions(),
                  spec.getMergeMode().isEmpty()
                      ? MergeMode.FULL
                      : MergeMode.valueOf(spec.getMergeMode()),
                  spec.getMaxClockSkewMs(),
                  false),
              logs.logFor(spec));
    }
    Set<Integer> desired = new HashSet<>(a.getPartitionsList());
    for (int p : engine.hosted()) {
      if (!desired.contains(p)) {
        engine.unhost(p);
        inflight.remove(spec.getName() + "/" + p);
      }
    }
    long serving = a.getServingVersion();
    long target = a.getTargetVersion();
    for (int p : desired) {
      PartitionReplica r = engine.host(p);
      String key = spec.getName() + "/" + p;
      if (serving != 0 && r.currentVersion() != serving) {
        // Get onto the serving version first (bootstrap, or the swap after a push).
        if (r.preparedVersion() == serving) {
          engine.swap(serving, List.of(p));
        } else {
          prepare(engine, key, p, serving);
        }
      } else if (target != 0 && r.currentVersion() != target && r.preparedVersion() != target) {
        prepare(engine, key, p, target);
      }
    }
  }

  private void prepare(StoreEngine engine, String key, int partition, long version) {
    Long running = inflight.putIfAbsent(key, version);
    if (running != null) {
      return; // something is already loading for this partition; check again next tick
    }
    CompletableFuture<Void> f = engine.prepare(version, List.of(partition));
    f.whenComplete((v, e) -> inflight.remove(key, version));
  }

  @Override
  public void close() {
    ticker.shutdownNow();
    try {
      ticker.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
