package io.strata.controller;

import io.strata.common.Murmur3;
import io.strata.proto.v1.ClusterView;
import io.strata.proto.v1.HeartbeatRequest;
import io.strata.proto.v1.HeartbeatResponse;
import io.strata.proto.v1.PartitionView;
import io.strata.proto.v1.PushStatus;
import io.strata.proto.v1.ReplicaStatus;
import io.strata.proto.v1.ReplicaView;
import io.strata.proto.v1.StoreAssignment;
import io.strata.proto.v1.StoreSpec;
import io.strata.proto.v1.StoreView;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.LongSupplier;

/**
 * All controller decisions, as a plain synchronized object (no I/O) so it can be unit-tested with a
 * fake clock. See docs/adr/0005-controller.md.
 *
 * <ul>
 *   <li><b>Membership:</b> a node is live if it heartbeated within {@code nodeTimeoutMs}.
 *   <li><b>Placement:</b> rendezvous (highest-random-weight) hashing. Each live node gets a score
 *       per (store, partition), and the top {@code RF} nodes host it. When a node leaves, only its
 *       partitions move, and when it returns they move back. No placement state needs storing.
 *   <li><b>Push lifecycle:</b> {@code startPush} sets a target version. Nodes prepare it, and once
 *       every live assigned replica of every partition reports it prepared (or serving), the
 *       serving version flips and nodes swap on their next heartbeat.
 * </ul>
 */
public final class ControllerState {

  private record Node(
      String id, String address, long lastSeen, Map<String, ReplicaStatus> status) {}

  private static final class Store {
    final StoreSpec spec;
    long serving;
    long target;
    final List<Long> superseded = new ArrayList<>();

    Store(StoreSpec spec) {
      this.spec = spec;
    }
  }

  private final LongSupplier clock;
  private final long nodeTimeoutMs;
  private final Map<String, Node> nodes = new HashMap<>();
  private final Map<String, Store> stores = new LinkedHashMap<>();
  private long epoch = 1;
  private List<String> lastLive = List.of();

  public ControllerState(LongSupplier clock, long nodeTimeoutMs) {
    this.clock = clock;
    this.nodeTimeoutMs = nodeTimeoutMs;
  }

  public synchronized void createStore(StoreSpec spec) {
    if (spec.getNumPartitions() == 0 || spec.getReplicationFactor() == 0) {
      throw new IllegalArgumentException("partitions and replication factor must be > 0");
    }
    Store existing = stores.get(spec.getName());
    if (existing != null) {
      if (!existing.spec.equals(spec)) {
        throw new IllegalArgumentException("store exists with a different spec: " + spec.getName());
      }
      return;
    }
    stores.put(spec.getName(), new Store(spec));
    epoch++;
  }

  public synchronized HeartbeatResponse heartbeat(HeartbeatRequest req) {
    Map<String, ReplicaStatus> status = new HashMap<>();
    for (ReplicaStatus r : req.getReplicasList()) {
      status.put(r.getStore() + "/" + r.getPartition(), r);
    }
    nodes.put(
        req.getNodeId(), new Node(req.getNodeId(), req.getAddress(), clock.getAsLong(), status));
    refreshMembership();
    stores.values().forEach(this::maybePromote);

    HeartbeatResponse.Builder resp = HeartbeatResponse.newBuilder();
    for (Store s : stores.values()) {
      StoreAssignment.Builder a =
          StoreAssignment.newBuilder()
              .setSpec(s.spec)
              .setServingVersion(s.serving)
              .setTargetVersion(s.target);
      for (int p = 0; p < s.spec.getNumPartitions(); p++) {
        if (replicasFor(s.spec, p).contains(req.getNodeId())) {
          a.addPartitions(p);
        }
      }
      resp.addStores(a);
    }
    return resp.build();
  }

  public synchronized PushStatus startPush(String store, long version) {
    Store s = require(store);
    if (version <= s.serving) {
      throw new IllegalArgumentException(
          "version %d is not newer than serving %d".formatted(version, s.serving));
    }
    if (s.target != 0 && s.target != version) {
      s.superseded.add(s.target); // a newer push replaces an unfinished one
    }
    s.target = version;
    epoch++;
    maybePromote(s);
    return status(s, version);
  }

  public synchronized PushStatus pushStatus(String store, long version) {
    return status(require(store), version);
  }

  public synchronized ClusterView view() {
    refreshMembership();
    ClusterView.Builder v = ClusterView.newBuilder().setEpoch(epoch);
    for (Store s : stores.values()) {
      StoreView.Builder sv = StoreView.newBuilder().setSpec(s.spec).setServingVersion(s.serving);
      for (int p = 0; p < s.spec.getNumPartitions(); p++) {
        PartitionView.Builder pv = PartitionView.newBuilder().setPartition(p);
        for (String id : replicasFor(s.spec, p)) {
          Node n = nodes.get(id);
          ReplicaStatus st = n.status().get(s.spec.getName() + "/" + p);
          long cur = st == null ? 0 : st.getCurrentVersion();
          pv.addReplicas(
              ReplicaView.newBuilder()
                  .setNodeId(id)
                  .setAddress(n.address())
                  .setCurrentVersion(cur)
                  .setReady(s.serving != 0 && cur == s.serving));
        }
        sv.addPartitions(pv);
      }
      v.addStores(sv);
    }
    return v.build();
  }

  /** Live node ids, sorted. */
  public synchronized List<String> liveNodes() {
    long now = clock.getAsLong();
    return nodes.values().stream()
        .filter(n -> now - n.lastSeen() <= nodeTimeoutMs)
        .map(Node::id)
        .sorted()
        .toList();
  }

  /** Top-RF live nodes for a partition by rendezvous hashing. */
  public synchronized List<String> replicasFor(StoreSpec spec, int partition) {
    long key =
        Murmur3.hash64(
            Murmur3.hash32(spec.getName().getBytes(StandardCharsets.UTF_8), 17), partition);
    return liveNodes().stream()
        .sorted(
            Comparator.comparingLong(
                    (String id) ->
                        Murmur3.hash64(
                            Murmur3.hash32(id.getBytes(StandardCharsets.UTF_8), 31), key))
                .reversed()
                .thenComparing(Comparator.naturalOrder()))
        .limit(spec.getReplicationFactor())
        .toList();
  }

  public synchronized long epoch() {
    return epoch;
  }

  private void refreshMembership() {
    List<String> live = liveNodes();
    if (!live.equals(lastLive)) {
      lastLive = live;
      epoch++;
    }
  }

  private void maybePromote(Store s) {
    if (s.target == 0) {
      return;
    }
    if (readyPartitions(s, s.target) == s.spec.getNumPartitions()) {
      s.serving = s.target;
      s.target = 0;
      epoch++;
    }
  }

  private int readyPartitions(Store s, long version) {
    int ready = 0;
    for (int p = 0; p < s.spec.getNumPartitions(); p++) {
      List<String> replicas = replicasFor(s.spec, p);
      boolean all = !replicas.isEmpty();
      for (String id : replicas) {
        ReplicaStatus st = nodes.get(id).status().get(s.spec.getName() + "/" + p);
        if (st == null
            || (st.getPreparedVersion() != version && st.getCurrentVersion() != version)) {
          all = false;
          break;
        }
      }
      if (all) {
        ready++;
      }
    }
    return ready;
  }

  private PushStatus status(Store s, long version) {
    String state;
    if (version == s.serving) {
      state = "SERVING";
    } else if (version == s.target) {
      state = "PENDING";
    } else if (s.superseded.contains(version) || version < s.serving) {
      state = "SUPERSEDED";
    } else {
      state = "UNKNOWN";
    }
    return PushStatus.newBuilder()
        .setStore(s.spec.getName())
        .setVersion(version)
        .setState(state)
        .setPartitionsReady(readyPartitions(s, version))
        .setPartitionsTotal(s.spec.getNumPartitions())
        .build();
  }

  private Store require(String name) {
    Store s = stores.get(name);
    if (s == null) {
      throw new IllegalArgumentException("unknown store " + name);
    }
    return s;
  }

  /** For tests and logs: distinct nodes holding at least one replica. */
  public synchronized TreeSet<String> nodesInUse() {
    TreeSet<String> out = new TreeSet<>();
    for (Store s : stores.values()) {
      for (int p = 0; p < s.spec.getNumPartitions(); p++) {
        out.addAll(replicasFor(s.spec, p));
      }
    }
    return out;
  }
}
