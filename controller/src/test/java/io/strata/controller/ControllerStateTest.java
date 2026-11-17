package io.strata.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.strata.proto.v1.HeartbeatRequest;
import io.strata.proto.v1.HeartbeatResponse;
import io.strata.proto.v1.ReplicaStatus;
import io.strata.proto.v1.StoreAssignment;
import io.strata.proto.v1.StoreSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ControllerStateTest {

  final AtomicLong clock = new AtomicLong(1000);
  final ControllerState state = new ControllerState(clock::get, 3000);
  final StoreSpec spec =
      StoreSpec.newBuilder()
          .setName("s")
          .setNumPartitions(12)
          .setReplicationFactor(3)
          .setMergeMode("FULL")
          .build();

  HeartbeatResponse beat(String node, List<ReplicaStatus> status) {
    return state.heartbeat(
        HeartbeatRequest.newBuilder()
            .setNodeId(node)
            .setAddress(node + ":1")
            .addAllReplicas(status)
            .build());
  }

  List<Integer> partitions(HeartbeatResponse r) {
    return r.getStoresList().stream()
        .filter(a -> a.getSpec().getName().equals("s"))
        .findFirst()
        .map(StoreAssignment::getPartitionsList)
        .orElse(List.of());
  }

  @Test
  void everyPartitionGetsRfDistinctLiveReplicasSpreadAcrossNodes() {
    // Rendezvous hashing balances statistically, so check balance with many partitions.
    StoreSpec big = spec.toBuilder().setName("big").setNumPartitions(240).build();
    state.createStore(big);
    Map<String, Integer> load = new HashMap<>();
    for (String n : List.of("n1", "n2", "n3", "n4", "n5")) {
      beat(n, List.of());
    }
    for (int p = 0; p < 240; p++) {
      List<String> r = state.replicasFor(big, p);
      assertThat(r).hasSize(3).doesNotHaveDuplicates();
      r.forEach(n -> load.merge(n, 1, Integer::sum));
    }
    // 720 replicas over 5 nodes = 144 each on average.
    assertThat(load.values()).allSatisfy(c -> assertThat(c).isBetween(110, 180));
  }

  @Test
  void deadNodesPartitionsMoveAndOthersStayPut() {
    state.createStore(spec);
    for (String n : List.of("n1", "n2", "n3", "n4", "n5")) {
      beat(n, List.of());
    }
    Map<Integer, List<String>> before = new HashMap<>();
    for (int p = 0; p < 12; p++) {
      before.put(p, state.replicasFor(spec, p));
    }
    clock.addAndGet(5000); // n5 stops heartbeating
    for (String n : List.of("n1", "n2", "n3", "n4")) {
      beat(n, List.of());
    }
    for (int p = 0; p < 12; p++) {
      List<String> after = state.replicasFor(spec, p);
      assertThat(after).hasSize(3).doesNotContain("n5");
      Set<String> kept = new HashSet<>(before.get(p));
      kept.remove("n5");
      assertThat(after).containsAll(kept); // only the dead node's slot changed
    }
  }

  @Test
  void pushPromotesOnlyWhenAllReplicasArePrepared() {
    state.createStore(spec);
    List<String> nodes = List.of("n1", "n2", "n3");
    nodes.forEach(n -> beat(n, List.of()));
    state.startPush("s", 1);
    assertThat(state.pushStatus("s", 1).getState()).isEqualTo("PENDING");

    // n1 and n2 prepared; n3 not yet.
    for (String n : List.of("n1", "n2")) {
      HeartbeatResponse r = beat(n, List.of());
      beat(n, statuses(partitions(r), 0, 1));
    }
    assertThat(state.pushStatus("s", 1).getState()).isEqualTo("PENDING");
    HeartbeatResponse r3 = beat("n3", List.of());
    assertThat(r3.getStores(0).getTargetVersion()).isEqualTo(1);
    HeartbeatResponse after = beat("n3", statuses(partitions(r3), 0, 1));
    assertThat(state.pushStatus("s", 1).getState()).isEqualTo("SERVING");
    assertThat(after.getStores(0).getServingVersion()).isEqualTo(1);
    assertThat(after.getStores(0).getTargetVersion()).isZero();
  }

  @Test
  void deadReplicaDoesNotBlockAPush() {
    state.createStore(spec);
    List.of("n1", "n2", "n3", "n4").forEach(n -> beat(n, List.of()));
    state.startPush("s", 1);
    clock.addAndGet(5000); // n4 dies before preparing
    for (String n : List.of("n1", "n2", "n3")) {
      HeartbeatResponse r = beat(n, List.of());
      beat(n, statuses(partitions(r), 0, 1));
    }
    assertThat(state.pushStatus("s", 1).getState()).isEqualTo("SERVING");
  }

  @Test
  void rejectsOldVersionsAndSupersedesUnfinishedPushes() {
    state.createStore(spec);
    state.startPush("s", 2);
    state.startPush("s", 3);
    assertThat(state.pushStatus("s", 2).getState()).isEqualTo("SUPERSEDED");
    assertThatThrownBy(() -> state.createStore(spec.toBuilder().setNumPartitions(4).build()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void viewMarksOnlyServingReplicasReady() {
    state.createStore(spec);
    HeartbeatResponse r = beat("n1", List.of());
    beat("n2", List.of());
    beat("n3", List.of());
    state.startPush("s", 1);
    for (String n : List.of("n1", "n2", "n3")) {
      beat(n, statuses(partitions(beat(n, List.of())), 0, 1));
    }
    // Promoted, but nobody has swapped yet.
    assertThat(state.view().getStores(0).getPartitions(0).getReplicasList())
        .noneMatch(x -> x.getReady());
    beat("n1", statuses(partitions(r), 1, 0));
    assertThat(state.view().getStores(0).getPartitions(partitions(r).get(0)).getReplicasList())
        .anyMatch(x -> x.getReady() && x.getNodeId().equals("n1"));
  }

  static List<ReplicaStatus> statuses(List<Integer> parts, long current, long prepared) {
    List<ReplicaStatus> out = new ArrayList<>();
    for (int p : parts) {
      out.add(
          ReplicaStatus.newBuilder()
              .setStore("s")
              .setPartition(p)
              .setCurrentVersion(current)
              .setPreparedVersion(prepared)
              .build());
    }
    return out;
  }
}
