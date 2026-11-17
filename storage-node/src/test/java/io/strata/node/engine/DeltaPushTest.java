package io.strata.node.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.strata.proto.v1.PushType;
import io.strata.proto.v1.VersionManifest;
import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A chain of delta pushes must serve exactly what a full push of the same snapshot serves. */
class DeltaPushTest {

  @TempDir Path tmp;

  @Test
  void deltaChainEqualsFullPush() throws Exception {
    try (EngineFixture f = new EngineFixture(tmp, new StoreEngine.Config("s", 3), null)) {
      SyntheticDataset s0 = SyntheticDataset.initial(3_000, 24, 5);
      SyntheticDataset s1 = s0.next(0.05, 0.2);
      SyntheticDataset s2 = s1.next(0.10, 0.3); // deleted keys may come back here
      f.full(s0, 1, 100);
      VersionManifest d1 = f.push.pushDelta(s0, s1, 1, 2, 200);
      VersionManifest d2 = f.push.pushDelta(s1, s2, 2, 3, 300);
      assertThat(d1.getType()).isEqualTo(PushType.DELTA);
      assertThat(d1.getStats().getUpserts()).isBetween(80L, 180L);
      assertThat(d1.getStats().getDeletes()).isBetween(10L, 60L);
      assertThat(d2.getStats().getBytesWritten())
          .isLessThan(f.full(s2, 99, 300).getStats().getBytesWritten() / 3);

      f.engine.load(1).get(30, TimeUnit.SECONDS);
      long cachedAfterV1 = f.cache.bytesDownloaded();
      f.engine.load(2).get(30, TimeUnit.SECONDS);
      f.engine.load(3).get(30, TimeUnit.SECONDS);
      // The delta versions only downloaded their small delta files.
      assertThat(f.cache.bytesDownloaded() - cachedAfterV1)
          .isEqualTo(d1.getStats().getBytesWritten() + d2.getStats().getBytesWritten());

      for (long i = 0; i < 3_000; i++) {
        Lookup r = f.engine.get(SyntheticDataset.key(i));
        byte[] expected = s2.valueOf(i);
        assertThat(r.version()).isEqualTo(3);
        if (expected == null) {
          assertThat(r.found()).as("key %d should be deleted", i).isFalse();
        } else {
          assertThat(r.found()).as("key %d", i).isTrue();
          assertThat(Arrays.equals(r.value(), expected)).as("key %d", i).isTrue();
        }
      }
    }
  }
}
