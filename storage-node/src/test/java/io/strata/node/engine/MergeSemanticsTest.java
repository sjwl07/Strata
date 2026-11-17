package io.strata.node.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.common.InMemoryUpdateLog;
import io.strata.proto.v1.UpdateEvent;
import io.strata.push.MapDataset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The merge rules, one scenario per test. Times are milliseconds on a fake clock that also drives
 * the log's append time. One partition keeps the scenarios easy to follow.
 */
class MergeSemanticsTest {

  @TempDir Path tmp;
  final AtomicLong clock = new AtomicLong();
  // The first read at each offset comes back empty although records exist, like Kafka's poll.
  // Found the hard way: replay used to stop at the first empty read (journal, "Real Kafka").
  final InMemoryUpdateLog log = new InMemoryUpdateLog(1, clock::get, true);

  EngineFixture fixture(MergeMode mode) throws Exception {
    return new EngineFixture(tmp, new StoreEngine.Config("s", 1, mode, 0, false), log);
  }

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  void put(String key, String value, long ts, long appendAt) {
    clock.set(appendAt);
    log.append(
        0,
        UpdateEvent.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setValue(ByteString.copyFromUtf8(value))
            .setTs(ts)
            .setSourceId(1)
            .build());
  }

  void delete(String key, long ts, long appendAt) {
    clock.set(appendAt);
    log.append(
        0,
        UpdateEvent.newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setDelete(true)
            .setTs(ts)
            .setSourceId(1)
            .build());
  }

  String read(EngineFixture f, String key) throws Exception {
    // Wait until the partition worker has consumed everything in the log.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (f.engine.totalLag() > 0 && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    Thread.sleep(20);
    Lookup r = f.engine.get(b(key));
    return r.found() ? new String(r.value(), StandardCharsets.UTF_8) : null;
  }

  void push(EngineFixture f, long version, long watermark, Map<String, byte[]> rows)
      throws Exception {
    f.full(new MapDataset(rows), version, watermark);
    f.engine.load(version).get(30, TimeUnit.SECONDS);
  }

  /** CONCEPTS.md: cutoff 00:00 (t=1000), note at 03:00 (t=1300), new edition at 09:00 (t=1900). */
  @Test
  void ravi_fullModeKeepsUpdatesBetweenCutoffAndSwap() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 500, Map.of("ravi", b("111")));
      put("ravi", "222", 1300, 1300); // after the v2 cutoff, before v2 arrives
      assertThat(read(f, "ravi")).isEqualTo("222");
      push(f, 2, 1000, Map.of("ravi", b("111"))); // snapshot as of t=1000 still says 111
      assertThat(read(f, "ravi")).isEqualTo("222"); // replay re-applied the 03:00 note
    }
  }

  @Test
  void ravi_naiveAndLwwModesLoseTheUpdate() throws Exception {
    for (MergeMode mode : new MergeMode[] {MergeMode.NAIVE, MergeMode.LWW}) {
      try (EngineFixture f =
          new EngineFixture(
              tmp.resolve(mode.name()), new StoreEngine.Config("s", 1, mode, 0, false), log)) {
        push(f, 1, 500, Map.of("ravi", b("111")));
        put("ravi", "222", 1300 + mode.ordinal(), 1300 + mode.ordinal());
        assertThat(read(f, "ravi")).isEqualTo("222");
        push(f, 2, 1000, Map.of("ravi", b("111")));
        assertThat(read(f, "ravi")).as(mode.name()).isEqualTo("111"); // the anomaly
      }
    }
  }

  @Test
  void lateEventDoesNotOverwriteNewerValue() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 100, Map.of());
      put("k", "new", 300, 300);
      put("k", "old", 200, 350); // produced at t=200, delivered late
      assertThat(read(f, "k")).isEqualTo("new");
    }
  }

  @Test
  void naiveModeLetsLateEventRegressTheValue() throws Exception {
    try (EngineFixture f = fixture(MergeMode.NAIVE)) {
      push(f, 1, 100, Map.of());
      put("k", "new", 300, 300);
      put("k", "old", 200, 350);
      assertThat(read(f, "k")).isEqualTo("old");
    }
  }

  @Test
  void deleteIsNotUndoneByAnOlderLateUpdate() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 100, Map.of("k", b("v0")));
      delete("k", 300, 300);
      put("k", "stale", 250, 400); // older than the delete, delivered after it
      assertThat(read(f, "k")).isNull();
    }
  }

  @Test
  void eventAtOrBelowWatermarkIsIgnoredEvenForKeysAbsentFromTheSnapshot() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      // The snapshot (cutoff 1000) says "gone": the key was deleted at some point <= 1000.
      push(f, 1, 1000, Map.of());
      put("gone", "zombie", 900, 1100); // a late write from before the cutoff
      assertThat(read(f, "gone")).isNull();
    }
  }

  @Test
  void lwwModeResurrectsKeysAbsentFromTheSnapshot() throws Exception {
    try (EngineFixture f = fixture(MergeMode.LWW)) {
      push(f, 1, 1000, Map.of());
      put("gone", "zombie", 900, 1100);
      assertThat(read(f, "gone")).isEqualTo("zombie"); // why the watermark filter exists
    }
  }

  @Test
  void duplicatesAreIdempotent() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 100, Map.of());
      put("k", "a", 200, 200);
      put("k", "b", 300, 300);
      put("k", "a", 200, 310); // producer retry re-delivers an old record
      assertThat(read(f, "k")).isEqualTo("b");
    }
  }

  @Test
  void updatesArrivingBetweenPrepareAndSwapReachTheNewVersion() throws Exception {
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 100, Map.of("k", b("v1")));
      f.full(new MapDataset(Map.of("k", b("v2"))), 2, 500);
      f.engine.prepare(2).get(30, TimeUnit.SECONDS);
      put("k", "live", 600, 600); // dual-written to current and prepared versions
      assertThat(read(f, "k")).isEqualTo("live");
      f.engine.swap(2).get(30, TimeUnit.SECONDS);
      assertThat(read(f, "k")).isEqualTo("live");
    }
  }

  @Test
  void bootstrapReplaysFromWatermark() throws Exception {
    put("k", "early", 50, 50); // before the snapshot: already folded in
    put("k", "after", 700, 700); // after the snapshot cutoff
    try (EngineFixture f = fixture(MergeMode.FULL)) {
      push(f, 1, 500, Map.of("k", b("snap")));
      assertThat(read(f, "k")).isEqualTo("after");
    }
  }
}
