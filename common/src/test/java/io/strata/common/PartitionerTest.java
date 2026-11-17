package io.strata.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  @Test
  void murmurMatchesReferenceVectors() {
    // Published MurmurHash3_x86_32 test vectors.
    assertThat(Murmur3.hash32(new byte[0], 0)).isEqualTo(0);
    assertThat(Murmur3.hash32(new byte[0], 1)).isEqualTo(0x514E28B7);
    assertThat(Murmur3.hash32("hello".getBytes(StandardCharsets.UTF_8), 0)).isEqualTo(0x248bfa47);
    assertThat(Murmur3.hash32("Hello, world!".getBytes(StandardCharsets.UTF_8), 1234))
        .isEqualTo(0xfaf6cdb3);
  }

  @Test
  void spreadsKeysEvenly() {
    int parts = 16;
    int[] counts = new int[parts];
    for (int i = 0; i < 160_000; i++) {
      counts[Partitioner.partition(("k%012d".formatted(i)).getBytes(), parts)]++;
    }
    for (int c : counts) {
      assertThat(c).isBetween(9_000, 11_000);
    }
  }
}
