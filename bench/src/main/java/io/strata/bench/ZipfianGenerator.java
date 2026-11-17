package io.strata.bench;

import io.strata.common.Murmur3;
import java.util.SplittableRandom;

/**
 * Zipfian item picker over {@code [0, n)} with skew {@code theta} (0.99 is the usual "hot keys"
 * setting), from Gray et al., "Quickly Generating Billion-Record Synthetic Databases" (SIGMOD '94).
 * Item ranks are then scrambled with a hash so the hottest keys land in different partitions. Not
 * thread-safe; use one per thread.
 */
public final class ZipfianGenerator {

  private final long n;
  private final double theta;
  private final double alpha;
  private final double zetan;
  private final double eta;
  private final SplittableRandom random;
  private final boolean scramble;

  public ZipfianGenerator(long n, double theta, long seed, boolean scramble) {
    this.n = n;
    this.theta = theta;
    this.zetan = zeta(n, theta);
    double zeta2 = zeta(2, theta);
    this.alpha = 1.0 / (1.0 - theta);
    this.eta = (1 - Math.pow(2.0 / n, 1 - theta)) / (1 - zeta2 / zetan);
    this.random = new SplittableRandom(seed);
    this.scramble = scramble;
  }

  private static double zeta(long n, double theta) {
    double sum = 0;
    for (long i = 1; i <= n; i++) {
      sum += 1 / Math.pow(i, theta);
    }
    return sum;
  }

  /** Rank 0 is the most popular item. */
  public long nextRank() {
    double u = random.nextDouble();
    double uz = u * zetan;
    if (uz < 1.0) {
      return 0;
    }
    if (uz < 1.0 + Math.pow(0.5, theta)) {
      return 1;
    }
    return Math.min(n - 1, (long) (n * Math.pow(eta * u - eta + 1, alpha)));
  }

  public long next() {
    long rank = nextRank();
    return scramble ? Math.floorMod(Murmur3.hash64(rank, 0x2545F491L), n) : rank;
  }

  /** A copy with the same distribution (the zeta sum is the expensive part) and a new seed. */
  public ZipfianGenerator withSeed(long seed) {
    return new ZipfianGenerator(this, seed);
  }

  private ZipfianGenerator(ZipfianGenerator o, long seed) {
    this.n = o.n;
    this.theta = o.theta;
    this.alpha = o.alpha;
    this.zetan = o.zetan;
    this.eta = o.eta;
    this.scramble = o.scramble;
    this.random = new SplittableRandom(seed);
  }
}
