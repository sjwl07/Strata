package io.strata.node.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * One version of one partition: its own RocksDB instance in its own directory.
 *
 * <p><b>Lifetime.</b> Readers {@link #tryAcquire()} before reading and {@link #release()} after.
 * The owner holds one reference from creation until {@link #retire()}. The database is closed when
 * the count reaches zero, i.e. after the swap <i>and</i> after the last in-flight read finishes.
 * Closing a RocksDB handle while a JNI read is running would crash the JVM, so this matters.
 */
public final class PartitionVersion {

  private final RocksResources resources;
  private final long version;
  private final long watermark;
  private final Path dir;
  private final Options options;
  private final RocksDB db;
  private final AtomicInteger refs = new AtomicInteger(1);
  private final AtomicBoolean retired = new AtomicBoolean();

  private PartitionVersion(
      RocksResources resources, long version, long watermark, Path dir, Options o, RocksDB db) {
    this.resources = resources;
    this.version = version;
    this.watermark = watermark;
    this.dir = dir;
    this.options = o;
    this.db = db;
  }

  static PartitionVersion create(RocksResources res, long version, long watermark, Path dir)
      throws IOException {
    deleteRecursively(dir);
    Files.createDirectories(dir);
    Options o = res.newOptions();
    try {
      return new PartitionVersion(res, version, watermark, dir, o, RocksDB.open(o, dir.toString()));
    } catch (RocksDBException e) {
      o.close();
      throw new IOException("cannot open RocksDB at " + dir, e);
    }
  }

  public long version() {
    return version;
  }

  public long watermark() {
    return watermark;
  }

  /**
   * Ingests pushed SST files in order (later files win). Each file is hard-linked from the node's
   * file cache, then "moved" into RocksDB, so no data is copied and the cache keeps its copy for
   * the next delta.
   */
  void ingest(List<Path> files) throws IOException {
    Path staging = Files.createDirectories(dir.resolveSibling(dir.getFileName() + ".staging"));
    try (IngestExternalFileOptions opts =
        new IngestExternalFileOptions()
            .setMoveFiles(true)
            .setSnapshotConsistency(false)
            .setAllowGlobalSeqNo(true)
            .setAllowBlockingFlush(true)) {
      int n = 0;
      for (Path f : files) {
        Path link = staging.resolve("ingest-" + (n++) + ".sst");
        Files.deleteIfExists(link);
        try {
          Files.createLink(link, f);
        } catch (IOException | UnsupportedOperationException e) {
          Files.copy(f, link); // different filesystem: fall back to a copy
        }
        // One file per call: overlapping files must be layered in order, newest last.
        db.ingestExternalFile(List.of(link.toString()), opts);
      }
    } catch (RocksDBException e) {
      throw new IOException("ingest failed in " + dir, e);
    } finally {
      deleteRecursively(staging);
    }
  }

  /** Full compaction: merges ingested layers so reads touch one sorted run per level. */
  void compact() throws IOException {
    try {
      db.compactRange();
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  byte[] getRaw(byte[] key) throws RocksDBException {
    return db.get(key);
  }

  void putRaw(byte[] key, byte[] value) throws RocksDBException {
    db.put(resources.writeOptions(), key, value);
  }

  void deleteRaw(byte[] key) throws RocksDBException {
    db.delete(resources.writeOptions(), key);
  }

  RocksDB db() {
    return db;
  }

  boolean tryAcquire() {
    while (true) {
      int r = refs.get();
      if (r == 0) {
        return false; // already closed or closing; the caller re-reads the current pointer
      }
      if (refs.compareAndSet(r, r + 1)) {
        return true;
      }
    }
  }

  void release() {
    if (refs.decrementAndGet() == 0) {
      resources.scheduleClose(this::destroy);
    }
  }

  /** Drops the owner's reference. Idempotent. */
  void retire() {
    if (retired.compareAndSet(false, true)) {
      release();
    }
  }

  private void destroy() {
    db.close();
    options.close();
    try {
      deleteRecursively(dir);
    } catch (IOException ignored) {
      // best effort; the node wipes its data dir at startup anyway
    }
  }

  /** Deletes a tree; files that vanish concurrently (another cleaner) are not an error. */
  static void deleteRecursively(Path p) throws IOException {
    if (!Files.exists(p)) {
      return;
    }
    Files.walkFileTree(
        p,
        new java.nio.file.SimpleFileVisitor<>() {
          @Override
          public java.nio.file.FileVisitResult visitFile(
              Path f, java.nio.file.attribute.BasicFileAttributes a) throws IOException {
            Files.deleteIfExists(f);
            return java.nio.file.FileVisitResult.CONTINUE;
          }

          @Override
          public java.nio.file.FileVisitResult visitFileFailed(Path f, IOException e)
              throws IOException {
            if (e instanceof java.nio.file.NoSuchFileException) {
              return java.nio.file.FileVisitResult.CONTINUE;
            }
            throw e;
          }

          @Override
          public java.nio.file.FileVisitResult postVisitDirectory(Path d, IOException e)
              throws IOException {
            if (e != null && !(e instanceof java.nio.file.NoSuchFileException)) {
              throw e;
            }
            try {
              Files.deleteIfExists(d);
            } catch (java.nio.file.DirectoryNotEmptyException ignored) {
              // a concurrent writer added a file; the caller's next cleanup gets it
            }
            return java.nio.file.FileVisitResult.CONTINUE;
          }
        });
  }

  @Override
  public String toString() {
    return "v" + version + "@" + watermark;
  }
}
