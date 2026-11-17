package io.strata.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongFunction;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Fires requests on a fixed schedule (open loop) and records latency from each request's
 * <i>intended</i> start time.
 *
 * <p>Why open loop: a closed-loop tester waits for each response before sending the next, so when
 * the server stalls the tester stops sending and the stall shows up as one slow request instead of
 * hundreds. That's "coordinated omission" (Gil Tene), and it hides exactly the tail we want to
 * measure. Real users don't wait for each other.
 */
public final class OpenLoopLoad {

  public record Result(
      Histogram total, List<Histogram> perSecond, long sent, long errors, double seconds) {}

  private OpenLoopLoad() {}

  /**
   * @param op issues request {@code i} and returns a future that completes when it finishes
   *     (exceptionally on error). Must not block.
   */
  public static Result run(double ratePerSec, double seconds, LongFunction<CompletableFuture<?>> op)
      throws InterruptedException {
    Recorder recorder = new Recorder(3);
    Histogram total = new Histogram(TimeUnit.SECONDS.toNanos(60), 3);
    List<Histogram> perSecond = new ArrayList<>();
    AtomicLong errors = new AtomicLong();
    AtomicLong inFlight = new AtomicLong();
    long intervalNanos = (long) (1e9 / ratePerSec);
    long count = (long) (ratePerSec * seconds);
    long t0 = System.nanoTime();
    long nextSnapshot = t0 + 1_000_000_000L;
    for (long i = 0; i < count; i++) {
      long intended = t0 + i * intervalNanos;
      long now;
      while ((now = System.nanoTime()) < intended) {
        long wait = intended - now;
        if (wait > 50_000) {
          LockSupport.parkNanos(wait - 20_000);
        } else {
          Thread.onSpinWait();
        }
      }
      if (now >= nextSnapshot) {
        perSecond.add(recorder.getIntervalHistogram());
        nextSnapshot += 1_000_000_000L;
      }
      inFlight.incrementAndGet();
      CompletableFuture<?> f;
      try {
        f = op.apply(i);
      } catch (RuntimeException e) {
        f = CompletableFuture.failedFuture(e);
      }
      f.whenComplete(
          (r, e) -> {
            long latency = System.nanoTime() - intended;
            recorder.recordValue(Math.min(latency, TimeUnit.SECONDS.toNanos(59)));
            if (e != null) {
              errors.incrementAndGet();
            }
            inFlight.decrementAndGet();
          });
    }
    long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (inFlight.get() > 0 && System.nanoTime() < drainDeadline) {
      Thread.sleep(5);
    }
    perSecond.add(recorder.getIntervalHistogram());
    perSecond.forEach(total::add);
    return new Result(total, perSecond, count, errors.get(), (System.nanoTime() - t0) / 1e9);
  }

  public static String summary(Histogram h) {
    return "p50=%.3fms p99=%.3fms p999=%.3fms max=%.3fms"
        .formatted(ms(h, 50), ms(h, 99), ms(h, 99.9), h.getMaxValue() / 1e6);
  }

  public static double ms(Histogram h, double percentile) {
    return h.getValueAtPercentile(percentile) / 1e6;
  }
}
