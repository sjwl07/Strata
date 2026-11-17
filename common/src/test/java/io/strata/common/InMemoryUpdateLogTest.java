package io.strata.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.strata.proto.v1.UpdateEvent;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class InMemoryUpdateLogTest {

  static UpdateEvent ev(String k, long ts) {
    return UpdateEvent.newBuilder().setKey(ByteString.copyFromUtf8(k)).setTs(ts).build();
  }

  @Test
  void offsetsAreDenseAndReadable() throws Exception {
    InMemoryUpdateLog log = new InMemoryUpdateLog(2);
    assertThat(log.append(0, ev("a", 1))).isZero();
    assertThat(log.append(0, ev("b", 2))).isEqualTo(1);
    assertThat(log.endOffset(0)).isEqualTo(2);
    assertThat(log.read(0, 1, 10, Duration.ZERO)).hasSize(1);
    assertThat(log.read(1, 0, 10, Duration.ofMillis(5))).isEmpty();
  }

  @Test
  void offsetForTimeFindsFirstRecordAtOrAfter() {
    AtomicLong clock = new AtomicLong(100);
    InMemoryUpdateLog log = new InMemoryUpdateLog(1, clock::get);
    log.append(0, ev("a", 0)); // t=100
    clock.set(200);
    log.append(0, ev("b", 0)); // t=200
    clock.set(150); // clock going back must not make append times go back
    log.append(0, ev("c", 0)); // t=200
    assertThat(log.offsetForTime(0, 50)).isZero();
    assertThat(log.offsetForTime(0, 101)).isEqualTo(1);
    assertThat(log.offsetForTime(0, 200)).isEqualTo(1);
    assertThat(log.offsetForTime(0, 201)).isEqualTo(3);
  }
}
