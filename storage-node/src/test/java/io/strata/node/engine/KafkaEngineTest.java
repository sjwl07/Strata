package io.strata.node.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.common.LocalObjectStore;
import io.strata.kafka.KafkaUpdateLog;
import io.strata.proto.v1.UpdateEvent;
import io.strata.push.MapDataset;
import io.strata.push.PushJob;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** The replay-on-swap guarantee with a real Kafka broker. Runs only where Docker is available. */
@Testcontainers(disabledWithoutDocker = true)
class KafkaEngineTest {

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

  @TempDir Path tmp;

  static UpdateEvent ev(String k, String v, long ts) {
    return UpdateEvent.newBuilder()
        .setKey(ByteString.copyFromUtf8(k))
        .setValue(ByteString.copyFromUtf8(v))
        .setTs(ts)
        .setSourceId(1)
        .build();
  }

  @Test
  void swapKeepsUpdatesNewerThanTheWatermark() throws Exception {
    try (KafkaUpdateLog log =
            new KafkaUpdateLog(KAFKA.getBootstrapServers(), "ravi", 1, (short) 1);
        RocksResources res = new RocksResources(16L << 20, 16L << 20)) {
      LocalObjectStore objects = new LocalObjectStore(tmp.resolve("o"));
      var loader = Executors.newFixedThreadPool(2);
      StoreEngine engine =
          new StoreEngine(
              new StoreEngine.Config("ravi", 1),
              res,
              tmp.resolve("d"),
              objects,
              new FileCache(tmp.resolve("c"), objects),
              log,
              loader);
      engine.host(0);
      PushJob push = new PushJob(new PushJob.Config("ravi", 1, objects, tmp.resolve("w"), 1));
      long now = System.currentTimeMillis();
      push.pushFull(new MapDataset(Map.of("ravi", "111".getBytes())), 1, now - 60_000);
      engine.load(1).get(1, TimeUnit.MINUTES);
      long cutoff = System.currentTimeMillis();
      Thread.sleep(1500); // stay clear of the 1 s default skew bound
      log.append(0, ev("ravi", "222", System.currentTimeMillis()));
      push.pushFull(new MapDataset(Map.of("ravi", "111".getBytes())), 2, cutoff);
      engine.load(2).get(1, TimeUnit.MINUTES);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      Lookup r;
      do {
        Thread.sleep(50);
        r = engine.get("ravi".getBytes(StandardCharsets.UTF_8));
      } while (!new String(r.value(), StandardCharsets.UTF_8).equals("222")
          && System.nanoTime() < deadline);
      engine.close(); // stop partition workers before the log closes their consumers
      loader.shutdownNow();
      assertThat(r.version()).isEqualTo(2);
      assertThat(new String(r.value(), StandardCharsets.UTF_8)).isEqualTo("222");
    }
  }
}
