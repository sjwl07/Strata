package io.strata.sst;

import io.strata.common.StoredValue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/**
 * Writes one SST file. Keys must be added in strictly increasing unsigned-byte order (RocksDB's
 * default comparator); the writer checks this so a bad sort fails loudly at push time rather than
 * corrupting reads later.
 */
public final class SstWriter implements AutoCloseable {

  private final Path path;
  private final EnvOptions env = new EnvOptions();
  private final Options options = RocksFormat.writerOptions();
  private final SstFileWriter writer = new SstFileWriter(env, options);
  private byte[] lastKey;
  private long entries;

  public SstWriter(Path path) throws IOException {
    this.path = path;
    Files.createDirectories(path.toAbsolutePath().getParent());
    try {
      writer.open(path.toString());
    } catch (RocksDBException e) {
      close();
      throw new IOException("cannot open SST " + path, e);
    }
  }

  public void put(byte[] key, StoredValue value) throws IOException {
    checkOrder(key);
    try {
      writer.put(key, value.encode());
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  /** A batch-level delete (used by delta pushes): removes the key from the base layers. */
  public void delete(byte[] key) throws IOException {
    checkOrder(key);
    try {
      writer.delete(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  private void checkOrder(byte[] key) {
    if (lastKey != null && Arrays.compareUnsigned(lastKey, key) >= 0) {
      throw new IllegalArgumentException("keys must be strictly increasing");
    }
    lastKey = key;
    entries++;
  }

  public long entries() {
    return entries;
  }

  /** Finishes the file and returns its size, or 0 if no entries were written (no file). */
  public long finish() throws IOException {
    if (entries == 0) {
      return 0;
    }
    try {
      writer.finish();
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    return Files.size(path);
  }

  @Override
  public void close() {
    writer.close();
    options.close();
    env.close();
  }
}
