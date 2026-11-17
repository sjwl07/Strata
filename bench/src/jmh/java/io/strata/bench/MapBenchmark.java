package io.strata.bench;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Read-heavy (95% get / 5% put) throughput of four thread-safe map designs over 1M keys. Thread
 * counts are set on the command line ({@code -t}), see docs/notes/phase0-jmh.md.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class MapBenchmark {

  static final int KEYS = 1_000_000;

  @Param({"synchronized", "synchronizedMap", "concurrent", "rwlock"})
  public String impl;

  private Store store;

  interface Store {
    Integer get(Integer k);

    void put(Integer k, Integer v);
  }

  @Setup(Level.Trial)
  public void setUp() {
    store =
        switch (impl) {
          case "synchronized" -> new SynchronizedStore();
          case "synchronizedMap" -> new MapStore(Collections.synchronizedMap(new HashMap<>()));
          case "concurrent" -> new MapStore(new ConcurrentHashMap<>());
          case "rwlock" -> new RwLockStore();
          default -> throw new IllegalArgumentException(impl);
        };
    for (int i = 0; i < KEYS; i++) {
      store.put(i, i);
    }
  }

  @Benchmark
  public Integer readMostly() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int key = r.nextInt(KEYS);
    if (r.nextInt(100) < 5) {
      store.put(key, key + 1);
      return key;
    }
    // Returning the value hands it to JMH's Blackhole, so the JIT can't eliminate the read.
    return store.get(key);
  }

  static final class MapStore implements Store {
    private final Map<Integer, Integer> map;

    MapStore(Map<Integer, Integer> map) {
      this.map = map;
    }

    public Integer get(Integer k) {
      return map.get(k);
    }

    public void put(Integer k, Integer v) {
      map.put(k, v);
    }
  }

  static final class SynchronizedStore implements Store {
    private final Map<Integer, Integer> map = new HashMap<>();

    public synchronized Integer get(Integer k) {
      return map.get(k);
    }

    public synchronized void put(Integer k, Integer v) {
      map.put(k, v);
    }
  }

  static final class RwLockStore implements Store {
    private final Map<Integer, Integer> map = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Integer get(Integer k) {
      lock.readLock().lock();
      try {
        return map.get(k);
      } finally {
        lock.readLock().unlock();
      }
    }

    public void put(Integer k, Integer v) {
      lock.writeLock().lock();
      try {
        map.put(k, v);
      } finally {
        lock.writeLock().unlock();
      }
    }
  }
}
