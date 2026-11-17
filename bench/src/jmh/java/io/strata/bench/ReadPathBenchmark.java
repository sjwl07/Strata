package io.strata.bench;

import io.strata.common.LocalObjectStore;
import io.strata.node.engine.FileCache;
import io.strata.node.engine.Files2;
import io.strata.node.engine.Lookup;
import io.strata.node.engine.RocksResources;
import io.strata.node.engine.StoreEngine;
import io.strata.push.PushJob;
import io.strata.push.SyntheticDataset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of one engine point read (partition lookup, version ref-count, RocksDB get, decode), without
 * RPC. Used to find and fix read-path hotspots (Phase 6). Run with {@code -Pjmh.prof=gc} to see
 * bytes allocated per read.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ReadPathBenchmark {

  static final long KEYS = 200_000;

  Path dir;
  RocksResources resources;
  ExecutorService loader;
  StoreEngine engine;
  byte[][] keys;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    dir = Files.createTempDirectory("strata-readpath");
    LocalObjectStore objects = new LocalObjectStore(dir.resolve("o"));
    new PushJob(new PushJob.Config("b", 8, objects, dir.resolve("w"), 2))
        .pushFull(SyntheticDataset.initial(KEYS, 100, 1), 1, 1);
    resources = new RocksResources(128L << 20, 32L << 20);
    loader = Executors.newFixedThreadPool(2);
    engine =
        new StoreEngine(
            new StoreEngine.Config("b", 8),
            resources,
            dir.resolve("d"),
            objects,
            new FileCache(dir.resolve("c"), objects),
            null,
            loader);
    for (int p = 0; p < 8; p++) {
      engine.host(p);
    }
    engine.load(1).get();
    keys = new byte[4096][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = SyntheticDataset.key(ThreadLocalRandom.current().nextLong(KEYS));
    }
  }

  @Benchmark
  public Lookup get() throws Exception {
    return engine.get(keys[ThreadLocalRandom.current().nextInt(keys.length)]);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    engine.close();
    loader.shutdownNow();
    resources.close();
    Files2.deleteRecursively(dir);
  }
}
