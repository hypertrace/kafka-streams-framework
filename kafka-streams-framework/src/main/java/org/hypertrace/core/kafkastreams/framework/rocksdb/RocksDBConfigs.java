package org.hypertrace.core.kafkastreams.framework.rocksdb;

public class RocksDBConfigs {

  public static final String ROCKS_DB_PREFIX = "rocksdb.";

  // ####### Cache #######
  // Total rocksdb cache limit (block cache + memtables + index and filter blocks)
  public static final String CACHE_TOTAL_CAPACITY = rocksdbPrefix("cache.total.capacity");
  public static final Long DEFAULT_CACHE_TOTAL_CAPACITY = 128 * 1024 * 1024l;

  public static final String CACHE_BLOCK_CACHE_RATIO = rocksdbPrefix("cache.block.cache.ratio");
  public static final Double DEFAULT_CACHE_BLOCK_CACHE_RATIO = 0.4;
  public static final String CACHE_WRITE_BUFFERS_RATIO = rocksdbPrefix("cache.write.buffers.ratio");
  public static final Double DEFAULT_CACHE_WRITE_BUFFERS_RATIO = 0.4;
  public static final String CACHE_HIGH_PRIORITY_POOL_RATIO = rocksdbPrefix(
      "cache.high.priority.pool.ratio");
  public static final Double DEFAULT_CACHE_HIGH_PRIORITY_POOL_RATIO = 0.2;

  // ####### Block Cache (Read cache) #######
  public static final String BLOCK_SIZE = rocksdbPrefix("block.size");

  // ####### Memtables (Write cache) #######
  public static final String WRITE_BUFFER_SIZE = rocksdbPrefix("write.buffer.size");
  public static final String MAX_WRITE_BUFFERS = rocksdbPrefix("max.write.buffers");

  // ####### Index and filter blocks cache #######
  public static final String CACHE_INDEX_AND_FILTER_BLOCKS = rocksdbPrefix(
      "cache.index.and.filter.blocks");

  public static final String COMPRESSION_TYPE = rocksdbPrefix("compression.type");
  public static final String COMPACTION_STYLE = rocksdbPrefix("compaction.style");
  public static final String DIRECT_READS_ENABLED = rocksdbPrefix("direct.reads.enabled");
  public static final String OPTIMIZE_FOR_POINT_LOOKUPS = rocksdbPrefix("optimize.point.lookups");

  public static final String LOG_LEVEL_CONFIG = rocksdbPrefix("log.level");

  public static String rocksdbPrefix(String configKey) {
    return ROCKS_DB_PREFIX + configKey;
  }
}
