package io.strata.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orders the replicas of a partition: the first gets the request, the next ones are used for
 * hedging and failover.
 */
public interface ReplicaSelector {

  List<ReplicaStats> rank(List<ReplicaStats> replicas);

  /** Selector strategies compared in Q3. */
  enum Strategy {
    ROUND_ROBIN,
    RANDOM,
    LEAST_OUTSTANDING,
    C3
  }

  static ReplicaSelector of(Strategy s, double concurrencyWeight) {
    return switch (s) {
      case ROUND_ROBIN -> new RoundRobin();
      case RANDOM -> new RandomOrder();
      case LEAST_OUTSTANDING -> new LeastOutstanding();
      case C3 -> new C3(concurrencyWeight);
    };
  }

  /** Rotates the starting replica: even spread, blind to load. */
  final class RoundRobin implements ReplicaSelector {
    private final AtomicLong next = new AtomicLong();

    @Override
    public List<ReplicaStats> rank(List<ReplicaStats> replicas) {
      int n = replicas.size();
      int start = (int) Math.floorMod(next.getAndIncrement(), (long) n);
      List<ReplicaStats> out = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        out.add(replicas.get((start + i) % n));
      }
      return out;
    }
  }

  /** Uniformly random order. */
  final class RandomOrder implements ReplicaSelector {
    @Override
    public List<ReplicaStats> rank(List<ReplicaStats> replicas) {
      List<ReplicaStats> out = new ArrayList<>(replicas);
      java.util.Collections.shuffle(out, ThreadLocalRandom.current());
      return out;
    }
  }

  /** Fewest requests in flight from this client first (ties broken randomly). */
  final class LeastOutstanding implements ReplicaSelector {
    @Override
    public List<ReplicaStats> rank(List<ReplicaStats> replicas) {
      List<ReplicaStats> out = new ArrayList<>(replicas);
      java.util.Collections.shuffle(out, ThreadLocalRandom.current());
      out.sort(Comparator.comparingInt(ReplicaStats::outstanding));
      return out;
    }
  }

  /**
   * C3 replica ranking (Suresh et al., "C3: Cutting Tail Latency in Cloud Data Stores via Adaptive
   * Replica Selection", NSDI '15). Score, lower is better:
   *
   * <pre>  Ψ = R − S + q̂³ · S,   q̂ = 1 + outstanding · w + q</pre>
   *
   * R: EWMA response time seen by the client; S: EWMA service time reported by the server (so R − S
   * approximates network + queueing delay); q: EWMA queue depth reported by the server;
   * outstanding: this client's requests in flight to it; w: concurrency compensation (roughly the
   * number of clients, since others are also sending to the same server). The cube penalises long
   * queues much more than a linear term, so load spreads before a queue builds. Replicas with no
   * samples yet score 0, so they get tried. C3's second half (per-server rate limiting) isn't
   * implemented; see the Q3 write-up.
   */
  final class C3 implements ReplicaSelector {
    private final double w;

    public C3(double concurrencyWeight) {
      this.w = concurrencyWeight;
    }

    public double score(ReplicaStats r) {
      if (r.samples() == 0) {
        return 0;
      }
      double s = Math.max(r.serviceMs(), 0.01);
      double qHat = 1 + r.outstanding() * w + r.queue();
      return r.responseMs() - s + qHat * qHat * qHat * s;
    }

    @Override
    public List<ReplicaStats> rank(List<ReplicaStats> replicas) {
      List<ReplicaStats> out = new ArrayList<>(replicas);
      java.util.Collections.shuffle(out, ThreadLocalRandom.current()); // random tie-break
      double[] scores = new double[out.size()];
      for (int i = 0; i < out.size(); i++) {
        scores[i] = score(out.get(i));
      }
      Integer[] idx = new Integer[out.size()];
      for (int i = 0; i < idx.length; i++) {
        idx[i] = i;
      }
      java.util.Arrays.sort(idx, Comparator.comparingDouble(i -> scores[i]));
      List<ReplicaStats> sorted = new ArrayList<>(out.size());
      for (int i : idx) {
        sorted.add(out.get(i));
      }
      return sorted;
    }
  }
}
