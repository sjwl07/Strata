package io.strata.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class StoredValueTest {

  static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void roundTripsThroughEncoding() {
    StoredValue v = StoredValue.live(42, 7, b("hello"));
    StoredValue d = StoredValue.decode(v.encode());
    assertThat(d.ts()).isEqualTo(42);
    assertThat(d.sourceId()).isEqualTo(7);
    assertThat(d.tombstone()).isFalse();
    assertThat(d.value()).isEqualTo(b("hello"));
    assertThat(StoredValue.tsOf(v.encode())).isEqualTo(42);
    assertThat(StoredValue.decode(StoredValue.tombstone(5, 1).encode()).tombstone()).isTrue();
  }

  @Test
  void newerTimestampWins() {
    assertThat(StoredValue.live(2, 0, b("a"))).isGreaterThan(StoredValue.live(1, 9, b("z")));
  }

  @Test
  void tiesBreakDeterministically() {
    StoredValue s1 = StoredValue.live(5, 1, b("x"));
    StoredValue s2 = StoredValue.live(5, 2, b("a"));
    assertThat(s2).isGreaterThan(s1);
    assertThat(StoredValue.tombstone(5, 1)).isGreaterThan(StoredValue.live(5, 1, b("x")));
    assertThat(StoredValue.live(5, 1, b("b"))).isGreaterThan(StoredValue.live(5, 1, b("a")));
    assertThat(StoredValue.live(5, StoredValue.BATCH_SOURCE, b("a")))
        .isGreaterThan(StoredValue.live(5, 3, b("z")));
  }
}
