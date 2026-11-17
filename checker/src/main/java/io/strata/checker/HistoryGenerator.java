package io.strata.checker;

import io.strata.checker.History.Action;
import io.strata.checker.History.Append;
import io.strata.checker.History.Check;
import io.strata.checker.History.Event;
import io.strata.checker.History.Push;
import io.strata.checker.History.Restart;
import io.strata.checker.History.Swap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;

/** Generates random but reproducible (seeded) messy histories. */
public final class HistoryGenerator {

  /** Knobs for the kinds of mess to generate. Times are in ms. */
  public record Params(
      int numKeys,
      int numEvents,
      int meanGapMs,
      double lateProbability,
      int maxLatenessMs,
      double skewProbability,
      int maxSkewMs,
      double duplicateProbability,
      double deleteProbability,
      int numSources,
      int pushEveryMs,
      int minBuildDelayMs,
      int maxBuildDelayMs,
      int maxSwapDelayMs,
      int checkEveryMs,
      double restartProbabilityPerPush) {

    public static Params defaults() {
      return new Params(
          24, 1500, 10, 0.10, 3000, 0.05, 200, 0.03, 0.10, 3, 2500, 300, 2000, 400, 100, 0.3);
    }
  }

  private HistoryGenerator() {}

  public static History generate(Params p, long seed) {
    SplittableRandom r = new SplittableRandom(seed);
    List<Action> actions = new ArrayList<>();
    long t = 1000;
    for (int id = 0; id < p.numEvents(); id++) {
      t += 1 + r.nextInt(2 * p.meanGapMs());
      int key = r.nextInt(p.numKeys());
      long ts = t;
      long appendAt = t + r.nextInt(5);
      double u = r.nextDouble();
      if (u < p.lateProbability()) {
        appendAt = t + 1 + r.nextInt(p.maxLatenessMs());
      } else if (u < p.lateProbability() + p.skewProbability()) {
        appendAt = Math.max(0, t - 1 - r.nextInt(p.maxSkewMs())); // producer clock ahead
      }
      Event e =
          new Event(
              id, key, ts, 1 + r.nextInt(p.numSources()), r.nextDouble() < p.deleteProbability());
      actions.add(new Append(appendAt, e, false));
      if (r.nextDouble() < p.duplicateProbability()) {
        actions.add(new Append(appendAt + 1 + r.nextInt(1000), e, true));
      }
    }
    long end = t + p.maxLatenessMs() + 100;
    // Version 1 at time 0 with cutoff 0 (empty snapshot) so the store serves from the start.
    actions.add(new Push(0, 1, 0));
    actions.add(new Swap(0, 1));
    long version = 2;
    for (long at = p.pushEveryMs(); at < end; at += p.pushEveryMs()) {
      long arrive = at + r.nextInt(p.pushEveryMs() / 4);
      long cutoff =
          arrive - p.minBuildDelayMs() - r.nextInt(p.maxBuildDelayMs() - p.minBuildDelayMs() + 1);
      actions.add(new Push(arrive, version, cutoff));
      actions.add(new Swap(arrive + 1 + r.nextInt(p.maxSwapDelayMs()), version));
      if (r.nextDouble() < p.restartProbabilityPerPush()) {
        actions.add(new Restart(arrive + r.nextInt(p.pushEveryMs())));
      }
      version++;
    }
    for (long at = p.checkEveryMs(); at <= end + 1000; at += p.checkEveryMs()) {
      actions.add(new Check(at));
    }
    actions.add(new Check(end + 2000));
    // Stable sort by time; ties keep generation order (pushes before their swaps).
    actions.sort(Comparator.comparingLong(Action::at));
    return new History(seed, p.numKeys(), List.copyOf(actions));
  }
}
