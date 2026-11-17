package io.strata.fault;

import static org.assertj.core.api.Assertions.assertThat;

import io.strata.common.ServeHook;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FaultsTest {

  static long timeMs(ServeHook h) throws InterruptedException {
    long s = System.nanoTime();
    h.beforeServe(1);
    return (System.nanoTime() - s) / 1_000_000;
  }

  @Test
  void slowDiskDelaysAboutTheConfiguredFraction() throws Exception {
    ServeHook h = Faults.slowDisk(0.3, 2, 1);
    int slow = 0;
    for (int i = 0; i < 300; i++) {
      if (timeMs(h) >= 2) {
        slow++;
      }
    }
    assertThat(slow).isBetween(60, 130);
  }

  @Test
  void pausesBlockOnlyInsideThePauseWindow() throws Exception {
    AtomicLong clock = new AtomicLong(0);
    ServeHook h = Faults.pauses(1000, 20, clock::get);
    clock.set(500);
    assertThat(timeMs(h)).isLessThan(5);
    clock.set(1005); // 5 ms into a 20 ms pause
    assertThat(timeMs(h)).isBetween(12L, 40L);
  }
}
