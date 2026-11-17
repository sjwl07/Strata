package io.strata.node.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.strata.push.SyntheticDataset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VersionSwapTest {

  @TempDir Path tmp;

  @Test
  void servesEveryKeyOfAPushedVersion() throws Exception {
    try (EngineFixture f = new EngineFixture(tmp, new StoreEngine.Config("s", 4), null)) {
      SyntheticDataset ds = SyntheticDataset.initial(5_000, 32, 1);
      f.full(ds, 1, 1000);
      assertThatThrownBy(() -> f.engine.get(SyntheticDataset.key(0)))
          .isInstanceOf(NotServingException.class);
      f.engine.load(1).get(30, TimeUnit.SECONDS);
      for (long i = 0; i < 5_000; i++) {
        Lookup r = f.engine.get(SyntheticDataset.key(i));
        assertThat(r.found()).isTrue();
        assertThat(r.value()).isEqualTo(ds.valueOf(i));
        assertThat(r.version()).isEqualTo(1);
      }
      assertThat(f.engine.get("nope".getBytes()).found()).isFalse();
    }
  }

  @Test
  void readersNeverFailOrSeeMixedDataDuringSwaps() throws Exception {
    try (EngineFixture f = new EngineFixture(tmp, new StoreEngine.Config("s", 2), null)) {
      SyntheticDataset v1 = SyntheticDataset.initial(2_000, 16, 7);
      SyntheticDataset v2 = v1.next(1.0, 0); // every value changes
      f.full(v1, 1, 1000);
      f.full(v2, 2, 2000);
      f.engine.load(1).get(30, TimeUnit.SECONDS);

      AtomicBoolean stop = new AtomicBoolean();
      AtomicLong reads = new AtomicLong();
      List<Throwable> errors = new ArrayList<>();
      List<Thread> readers = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        Thread th =
            new Thread(
                () -> {
                  long i = 0;
                  while (!stop.get()) {
                    long k = (i++ * 7919) % 2_000;
                    try {
                      Lookup r = f.engine.get(SyntheticDataset.key(k));
                      byte[] expected = r.version() == 1 ? v1.valueOf(k) : v2.valueOf(k);
                      if (!r.found() || !Arrays.equals(r.value(), expected)) {
                        throw new AssertionError("wrong value for " + k + " at v" + r.version());
                      }
                      reads.incrementAndGet();
                    } catch (Throwable e) {
                      synchronized (errors) {
                        errors.add(e);
                      }
                      return;
                    }
                  }
                });
        th.start();
        readers.add(th);
      }
      // Swap back and forth while readers hammer the store: each swap closes the old RocksDB.
      for (int round = 0; round < 6; round++) {
        long v = round % 2 == 0 ? 2 : 1;
        f.engine.load(v).get(30, TimeUnit.SECONDS);
        Thread.sleep(30);
      }
      stop.set(true);
      for (Thread th : readers) {
        th.join();
      }
      assertThat(errors).isEmpty();
      assertThat(reads.get()).isGreaterThan(1000);
    }
  }
}
