package io.strata.node;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.strata.common.ObjectStore;
import io.strata.common.ServeHook;
import io.strata.common.UpdateLog;
import io.strata.node.engine.FileCache;
import io.strata.node.engine.Lookup;
import io.strata.node.engine.NotServingException;
import io.strata.node.engine.RocksResources;
import io.strata.node.engine.StoreEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A storage node process: stores, the RocksDB resources they share, the SST file cache, and the
 * gRPC server.
 *
 * <p>Reads run on a <b>fixed-size platform-thread pool</b>, not virtual threads: RocksDB reads are
 * JNI calls (which pin virtual threads), and a bounded pool gives the node a real queue whose depth
 * it reports to clients (C3 needs that signal).
 */
public final class StorageNode implements AutoCloseable {

  public record Config(
      String nodeId,
      String host,
      int port,
      Path dataDir,
      int serveThreads,
      long blockCacheBytes,
      long writeBufferBytes) {
    public static Config local(String nodeId, Path dataDir) {
      return new Config(nodeId, "127.0.0.1", 0, dataDir, 8, 64L << 20, 32L << 20);
    }

    public Config withPort(int p) {
      return new Config(nodeId, host, p, dataDir, serveThreads, blockCacheBytes, writeBufferBytes);
    }

    public Config withServeThreads(int n) {
      return new Config(nodeId, host, port, dataDir, n, blockCacheBytes, writeBufferBytes);
    }
  }

  private final Config config;
  private final RocksResources resources;
  private final FileCache fileCache;
  private final ExecutorService loader;
  private final ThreadPoolExecutor serveExecutor;
  private final Map<String, StoreEngine> stores = new ConcurrentHashMap<>();
  private volatile ServeHook hook = ServeHook.NONE;
  private Server server;

  public StorageNode(Config config, ObjectStore objectStore) throws IOException {
    this.config = config;
    // Stateless restart (ADR 0002): local state is a cache of (pushed SSTs + log), so a restarted
    // node wipes it and bootstraps again rather than trusting files a crash may have left behind.
    io.strata.node.engine.Files2.deleteRecursively(config.dataDir());
    this.resources = new RocksResources(config.blockCacheBytes(), config.writeBufferBytes());
    this.fileCache = new FileCache(config.dataDir().resolve("sst-cache"), objectStore);
    this.loader = Executors.newFixedThreadPool(2, named(config.nodeId() + "-loader"));
    this.serveExecutor =
        new ThreadPoolExecutor(
            config.serveThreads(),
            config.serveThreads(),
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            named(config.nodeId() + "-serve"));
    this.objectStore = objectStore;
  }

  private final ObjectStore objectStore;

  private static java.util.concurrent.ThreadFactory named(String prefix) {
    AtomicInteger n = new AtomicInteger();
    return r -> {
      Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  public String nodeId() {
    return config.nodeId();
  }

  public StoreEngine addStore(StoreEngine.Config storeConfig, UpdateLog log) {
    StoreEngine engine =
        new StoreEngine(
            storeConfig,
            resources,
            config.dataDir().resolve("stores").resolve(storeConfig.name()),
            objectStore,
            fileCache,
            log,
            loader);
    if (stores.putIfAbsent(storeConfig.name(), engine) != null) {
      engine.close();
      throw new IllegalArgumentException("store exists: " + storeConfig.name());
    }
    return engine;
  }

  public StoreEngine store(String name) {
    return stores.get(name);
  }

  public Map<String, StoreEngine> stores() {
    return Map.copyOf(stores);
  }

  public FileCache fileCache() {
    return fileCache;
  }

  /** Replaces the per-request hook (fault injection). */
  public void setServeHook(ServeHook hook) {
    this.hook = hook == null ? ServeHook.NONE : hook;
  }

  Lookup lookup(String store, byte[] key) throws NotServingException {
    StoreEngine e;
    if (store.isEmpty() && stores.size() == 1) {
      e = stores.values().iterator().next();
    } else {
      e = stores.get(store);
    }
    if (e == null) {
      throw new NotServingException("unknown store '" + store + "'");
    }
    return e.get(key);
  }

  public StorageNode start() throws IOException {
    StorageNodeService service =
        new StorageNodeService(
            this::lookup, () -> serveExecutor.getQueue().size(), keys -> hook.beforeServe(keys));
    server =
        NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.port()))
            .executor(serveExecutor)
            .addService(service)
            .build()
            .start();
    return this;
  }

  public int port() {
    return server.getPort();
  }

  public String address() {
    return config.host() + ":" + port();
  }

  public int queueDepth() {
    return serveExecutor.getQueue().size();
  }

  /** Drops cached SST files that no prepared or served version references any more. */
  public void pruneFileCache() throws IOException {
    Set<String> keep = new HashSet<>();
    stores.values().forEach(s -> keep.addAll(s.referencedFiles()));
    fileCache.retainOnly(keep);
  }

  @Override
  public void close() {
    if (server != null) {
      server.shutdownNow();
      try {
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    serveExecutor.shutdownNow();
    stores.values().forEach(StoreEngine::close);
    stores.clear();
    loader.shutdownNow();
    resources.close();
  }
}
