package io.strata.client;

/**
 * The hedging budget. Every primary request deposits {@code ratio} tokens (e.g. 0.05) and every
 * hedge spends one, so over time hedges can't exceed {@code ratio} × primaries. The cap bounds
 * bursts. Without a budget, hedging under overload doubles the load exactly when servers are
 * slowest, which is how hedging causes outages.
 */
public final class TokenBucket {

  private final double ratio;
  private final double capacity;
  private double tokens;

  public TokenBucket(double ratio, double capacity) {
    this.ratio = ratio;
    this.capacity = capacity;
    this.tokens = capacity;
  }

  public synchronized void deposit() {
    tokens = Math.min(capacity, tokens + ratio);
  }

  public synchronized boolean tryTake() {
    if (tokens >= 1) {
      tokens -= 1;
      return true;
    }
    return false;
  }
}
