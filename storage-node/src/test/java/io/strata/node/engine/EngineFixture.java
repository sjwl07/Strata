package io.strata.node.engine;

import io.strata.common.LocalObjectStore;
import io.strata.common.UpdateLog;
import io.strata.proto.v1.VersionManifest;
import io.strata.push.Dataset;
import io.strata.push.PushJob;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A single store on a temp dir, with a push job writing to a local object store. */
final class EngineFixture implements AutoCloseable {

  final LocalObjectStore objectStore;
  final RocksResources resources = new RocksResources(32L << 20, 16L << 20);
  final FileCache cache;
  final ExecutorService loader = Executors.newFixedThreadPool(2);
  final StoreEngine engine;
  final PushJob push;

  EngineFixture(Path tmp, StoreEngine.Config config, UpdateLog log) throws Exception {
    objectStore = new LocalObjectStore(tmp.resolve("objects"));
    cache = new FileCache(tmp.resolve("cache"), objectStore);
    engine =
        new StoreEngine(config, resources, tmp.resolve("data"), objectStore, cache, log, loader);
    push =
        new PushJob(
            new PushJob.Config(
                config.name(), config.numPartitions(), objectStore, tmp.resolve("work"), 2));
    for (int p = 0; p < config.numPartitions(); p++) {
      engine.host(p);
    }
  }

  VersionManifest full(Dataset ds, long version, long watermark) throws Exception {
    return push.pushFull(ds, version, watermark);
  }

  @Override
  public void close() {
    engine.close();
    loader.shutdownNow();
    resources.close();
  }
}
