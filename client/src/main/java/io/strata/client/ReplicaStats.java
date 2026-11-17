package io.strata.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * What the client knows about one replica (one server address): requests in flight, plus
 * exponentially weighted moving averages (EWMAs) of response time and of the server-reported
 * service time and queue depth. EWMA: {@code avg = alpha * sample + (1 - alpha) * avg}, i.e. a
 * cheap average that forgets old samples.
 */
public final class ReplicaStats {

  private static final double ALPHA = 0.2;

  private final String address;
  private final AtomicInteger outstanding = new AtomicInteger();
  // Guarded by this; written once per response, so contention is low.
  private double responseMs = -1;
  private double serviceMs = -1;
  private double queue = 0;
  private long samples;

  ReplicaStats(String address) {
    this.address = address;
  }

  public String address() {
    return address;
  }

  public int outstanding() {
    return outstanding.get();
  }

  void sent() {
    outstanding.incrementAndGet();
  }

  void finished() {
    outstanding.decrementAndGet();
  }

  synchronized void onResponse(double latencyMs, double serviceTimeMs, int queueDepth) {
    responseMs = responseMs < 0 ? latencyMs : ALPHA * latencyMs + (1 - ALPHA) * responseMs;
    if (serviceTimeMs >= 0) {
      serviceMs = serviceMs < 0 ? serviceTimeMs : ALPHA * serviceTimeMs + (1 - ALPHA) * serviceMs;
    }
    queue = ALPHA * queueDepth + (1 - ALPHA) * queue;
    samples++;
  }

  /** A failure counts as a very slow response, so the replica is avoided for a while. */
  synchronized void onFailure(double penaltyMs) {
    responseMs = responseMs < 0 ? penaltyMs : ALPHA * penaltyMs + (1 - ALPHA) * responseMs;
    samples++;
  }

  public synchronized double responseMs() {
    return responseMs;
  }

  public synchronized double serviceMs() {
    return serviceMs;
  }

  public synchronized double queue() {
    return queue;
  }

  public synchronized long samples() {
    return samples;
  }
}
