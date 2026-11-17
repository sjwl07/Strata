package io.strata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.proto.v1.UpdateEvent;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Runs only where Docker is available (CI); skipped otherwise. */
@Testcontainers(disabledWithoutDocker = true)
class KafkaUpdateLogTest {

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

  static UpdateEvent ev(String k, String v, long ts) {
    return UpdateEvent.newBuilder()
        .setKey(ByteString.copyFromUtf8(k))
        .setValue(ByteString.copyFromUtf8(v))
        .setTs(ts)
        .setSourceId(1)
        .build();
  }

  @Test
  void behavesLikeTheLogContractSays() throws Exception {
    try (KafkaUpdateLog log =
        new KafkaUpdateLog(KAFKA.getBootstrapServers(), "contract", 2, (short) 1)) {
      long t0 = System.currentTimeMillis();
      assertThat(log.append(0, ev("a", "1", 1))).isZero();
      assertThat(log.append(0, ev("b", "2", 2))).isEqualTo(1);
      assertThat(log.endOffset(0)).isEqualTo(2);
      assertThat(log.endOffset(1)).isZero();
      var recs = log.read(0, 1, 10, Duration.ofSeconds(5));
      assertThat(recs).hasSize(1);
      assertThat(recs.get(0).event().getKey().toStringUtf8()).isEqualTo("b");
      assertThat(recs.get(0).appendTimeMs()).isGreaterThanOrEqualTo(t0 - 1000);
      assertThat(log.offsetForTime(0, 0)).isZero();
      assertThat(log.offsetForTime(0, System.currentTimeMillis() + 60_000)).isEqualTo(2);
    }
  }
}
