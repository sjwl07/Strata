package io.strata.node.engine;

import io.strata.sst.RocksFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.WriteOptions;

/**
 * Native resources shared by every RocksDB instance on a node. A node opens one RocksDB per
 * (partition, version), so without sharing, each instance would get its own block cache and
 * memtable budget and memory would grow with the number of partitions. One {@link LRUCache} and one
 * {@link WriteBufferManager} cap the node's total off-heap memory instead.
 */
public final class RocksResources implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private final LRUCache cache;
  private final WriteBufferManager writeBuffers;
  private final WriteOptions writeOptions;
  private final ExecutorService cleaner =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "rocks-cleaner");
            t.setDaemon(true);
            return t;
          });

  public RocksResources(long blockCacheBytes, long writeBufferBytes) {
    this.cache = new LRUCache(blockCacheBytes);
    this.writeBuffers = new WriteBufferManager(writeBufferBytes, cache);
    // No WAL: a node's state can always be rebuilt from the pushed SSTs plus a log replay
    // (ADR 0002), so paying for a write-ahead log on every stream update buys nothing.
    this.writeOptions = new WriteOptions().setDisableWAL(true);
  }

  public static RocksResources defaults() {
    return new RocksResources(128L << 20, 64L << 20);
  }

  /** New options for one instance; the caller closes them after closing the DB. */
  Options newOptions() {
    BlockBasedTableConfig table = RocksFormat.tableConfig(cache);
    return new Options()
        .setCreateIfMissing(true)
        .setTableFormatConfig(table)
        .setWriteBufferManager(writeBuffers)
        .setWriteBufferSize(8L << 20)
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setInfoLogLevel(InfoLogLevel.WARN_LEVEL)
        .setKeepLogFileNum(1)
        .setMaxBackgroundJobs(2);
  }

  WriteOptions writeOptions() {
    return writeOptions;
  }

  /** Closes retired versions off the request path. */
  ExecutorService cleaner() {
    return cleaner;
  }

  @Override
  public void close() {
    cleaner.shutdown();
    try {
      cleaner.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    writeOptions.close();
    writeBuffers.close();
    cache.close();
  }
}
