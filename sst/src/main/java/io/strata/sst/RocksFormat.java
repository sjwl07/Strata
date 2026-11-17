package io.strata.sst;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Table settings shared by writers (push job) and readers (nodes). The SST files carry their own
 * format, so readers mostly need matching filter and block settings to read them efficiently.
 */
public final class RocksFormat {

  static {
    RocksDB.loadLibrary();
  }

  /** Point lookups dominate, so each SST carries a Bloom filter (~1% false positives). */
  public static final double BLOOM_BITS_PER_KEY = 10;

  public static final long BLOCK_SIZE = 16 * 1024;

  private RocksFormat() {}

  public static BlockBasedTableConfig tableConfig(Cache cache) {
    BlockBasedTableConfig t =
        new BlockBasedTableConfig()
            .setFilterPolicy(new BloomFilter(BLOOM_BITS_PER_KEY, false))
            .setBlockSize(BLOCK_SIZE)
            .setCacheIndexAndFilterBlocks(true)
            .setPinL0FilterAndIndexBlocksInCache(true)
            .setWholeKeyFiltering(true);
    if (cache != null) {
      t.setBlockCache(cache);
    }
    return t;
  }

  /** Options for {@code SstFileWriter}. The caller closes them. */
  public static Options writerOptions() {
    return new Options()
        .setTableFormatConfig(tableConfig(null))
        .setCompressionType(CompressionType.LZ4_COMPRESSION);
  }
}
