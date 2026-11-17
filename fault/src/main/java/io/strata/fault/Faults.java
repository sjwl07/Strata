package io.strata.fault;

import io.strata.common.ServeHook;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Realistic ways a server gets slow, as {@link ServeHook}s that run on the serving thread before
 * each read. Sleeping on that thread occupies a server worker, so queues build up behind a slow
 * request exactly as they would behind a slow disk read or a GC pause.
 */
public final class Faults {

  private Faults() {}

  /**
   * Slow disk: each request is delayed by {@code delayMs} with probability {@code probability}
   * (e.g. 30% of reads miss the cache and hit a degraded device).
   */
  public static ServeHook slowDisk(double probability, long delayMs, long seed) {
    SplittableRandom root = new SplittableRandom(seed);
    ThreadLocal<SplittableRandom> rnd = ThreadLocal.withInitial(() -> split(root));
    return keys -> {
      if (rnd.get().nextDouble() < probability) {
        TimeUnit.MILLISECONDS.sleep(delayMs);
      }
    };
  }

  /**
   * Stop-the-world pauses (GC-like): every {@code everyMs} the whole server freezes for {@code
   * pauseMs}. Requests arriving during a pause wait until it ends.
   */
  public static ServeHook pauses(long everyMs, long pauseMs, LongSupplier clockMs) {
    long phase = clockMs.getAsLong();
    return keys -> {
      long now = clockMs.getAsLong();
      long inCycle = Math.floorMod(now - phase, everyMs);
      if (inCycle < pauseMs) {
        TimeUnit.MILLISECONDS.sleep(pauseMs - inCycle);
      }
    };
  }

  /**
   * Noisy neighbour: random episodes (on average every {@code meanGapMs}) lasting {@code
   * episodeMs}, during which every request is slowed by {@code delayMs}, as when another tenant on
   * the host saturates the CPU or disk.
   */
  public static ServeHook noisyNeighbour(
      long meanGapMs, long episodeMs, long delayMs, long seed, LongSupplier clockMs) {
    SplittableRandom r = new SplittableRandom(seed);
    long[] episodes = new long[4096]; // start times, precomputed: exponential gaps
    long t = clockMs.getAsLong();
    for (int i = 0; i < episodes.length; i++) {
      t += (long) (-Math.log(1 - r.nextDouble()) * meanGapMs);
      episodes[i] = t;
    }
    return keys -> {
      long now = clockMs.getAsLong();
      int lo = 0;
      int hi = episodes.length - 1;
      while (lo < hi) { // last episode starting at or before now
        int mid = (lo + hi + 1) >>> 1;
        if (episodes[mid] <= now) {
          lo = mid;
        } else {
          hi = mid - 1;
        }
      }
      if (episodes[lo] <= now && now < episodes[lo] + episodeMs) {
        TimeUnit.MILLISECONDS.sleep(delayMs);
      }
    };
  }

  /** Runs several hooks in order. */
  public static ServeHook all(List<ServeHook> hooks) {
    return keys -> {
      for (ServeHook h : hooks) {
        h.beforeServe(keys);
      }
    };
  }

  private static synchronized SplittableRandom split(SplittableRandom root) {
    return root.split();
  }
}
