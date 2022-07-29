package org.hypertrace.core.kafkastreams.framework.rocksdb;

import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.BLOCK_SIZE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_HIGH_PRIORITY_POOL_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_TOTAL_CAPACITY;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_WRITE_BUFFERS_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DEFAULT_CACHE_HIGH_PRIORITY_POOL_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DEFAULT_CACHE_TOTAL_CAPACITY;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DEFAULT_CACHE_WRITE_BUFFERS_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.MAX_WRITE_BUFFERS;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.WRITE_BUFFER_SIZE;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.config.ConfigException;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a single control to manage and control the offheap memory used by RockDB RocksDB uses 3
 * major types of memory off the heap. This class creates a off-heap cache of configured capacity
 * for all the buffers (cache_index_and_filter_blocks, memtables and data blocks cached) and this
 * cache is shared across all the instances.
 * <p>
 * Read https://kafka.apache.org/26/documentation/streams/developer-guide/memory-mgmt.html#id3 for
 * more details.
 */
public class RocksDBCacheProvider {

  static class CacheWrapper {

    private Cache cache;
    private WriteBufferManager writeBufferManager;
    private long cacheTotalCapacity;
    private double writeBuffersRatio;
    private double highPriorityPoolRatio;
  }

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBCacheProvider.class);

  private static final RocksDBCacheProvider PROVIDER = new RocksDBCacheProvider();

  private final ConcurrentMap<String, CacheWrapper> cacheConfigPerApplicationId = new ConcurrentHashMap<>();

  /**
   * Singleton
   */
  private RocksDBCacheProvider() {
    // Do nothing - Lazy initialization
  }

  public static RocksDBCacheProvider get() {
    return PROVIDER;
  }

  private CacheWrapper getCacheWrapper(String applicationId) {
    return cacheConfigPerApplicationId.computeIfAbsent(applicationId,
        s -> new CacheWrapper());
  }

  protected synchronized void initCache(Options options, Map<String, Object> configs) {
    String applicationId = (String) configs.get("application.id");
    CacheWrapper cacheWrapper = getCacheWrapper(applicationId);

    if (cacheWrapper.cache == null) {
      cacheWrapper.cacheTotalCapacity = DEFAULT_CACHE_TOTAL_CAPACITY;
      if (configs.containsKey(CACHE_TOTAL_CAPACITY)) {
        cacheWrapper.cacheTotalCapacity = Long
            .valueOf(String.valueOf(configs.get(CACHE_TOTAL_CAPACITY)));
      }

      cacheWrapper.writeBuffersRatio = DEFAULT_CACHE_WRITE_BUFFERS_RATIO;
      if (configs.containsKey(CACHE_WRITE_BUFFERS_RATIO)) {
        cacheWrapper.writeBuffersRatio = Double
            .valueOf(String.valueOf(configs.get(CACHE_WRITE_BUFFERS_RATIO)));
      }

      if (cacheWrapper.writeBuffersRatio < 0.0 || cacheWrapper.writeBuffersRatio > 1.0) {
        throw new ConfigException(
            "Invalid high priority write buffers ratio configured. Config key: "
                + CACHE_WRITE_BUFFERS_RATIO + ", configured value: " + String.valueOf(
                configs.get(CACHE_WRITE_BUFFERS_RATIO) + ", Allowed value range: (0.0, 1.0)"));
      }

      cacheWrapper.highPriorityPoolRatio = DEFAULT_CACHE_HIGH_PRIORITY_POOL_RATIO;
      if (configs.containsKey(CACHE_HIGH_PRIORITY_POOL_RATIO)) {
        cacheWrapper.highPriorityPoolRatio = Double
            .valueOf(String.valueOf(configs.get(CACHE_HIGH_PRIORITY_POOL_RATIO)));
      }

      if (cacheWrapper.highPriorityPoolRatio < 0.0 || cacheWrapper.highPriorityPoolRatio > 1.0) {
        throw new ConfigException("Invalid high priority cache pool ratio configured. Config key: "
            + CACHE_HIGH_PRIORITY_POOL_RATIO + ", configured value: " + String.valueOf(
            configs.get(CACHE_HIGH_PRIORITY_POOL_RATIO) + ", Allowed value range: (0.0, 1.0)"));
      }

      double aggregatedRatio = cacheWrapper.writeBuffersRatio + cacheWrapper.highPriorityPoolRatio;
      if (aggregatedRatio > 1.0) {
        throw new ConfigException("Sum total of the cache ratios: " + aggregatedRatio
            + " is greater than 1.0."
            + " Configure all the cache ratios appropriately.");
      }

      // Strict capacity limit can't be used due to a bug in RocksDB
      // numShardBits = -1 means it is automatically determined
      // Read https://kafka.apache.org/26/documentation/streams/developer-guide/memory-mgmt.html#id3
      cacheWrapper.cache = new LRUCache(cacheWrapper.cacheTotalCapacity, -1, false,
          cacheWrapper.highPriorityPoolRatio);

      long writeBuffersTotalCapacity = (long) (cacheWrapper.writeBuffersRatio
          * cacheWrapper.cacheTotalCapacity);
      // makes write buffers are allocated from shared cache
      cacheWrapper.writeBufferManager = new WriteBufferManager(writeBuffersTotalCapacity,
          cacheWrapper.cache);

      LOG.info(
          "RocksDB shared cache initialized successfully. Total cache size(MB): {},"
              + " write buffers ratio: {}, high priority pool ratio: {}",
          cacheWrapper.cacheTotalCapacity / (1024 * 1024), cacheWrapper.writeBuffersRatio,
          cacheWrapper.highPriorityPoolRatio);
    }

    final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();

    // ######### Block cache (Read buffers) #########
    if (configs.containsKey(BLOCK_SIZE)) {
      tableConfig.setBlockSize(Long.valueOf(String.valueOf(configs.get(BLOCK_SIZE))));
    }

    // ######### Write buffers (memtables) #########
    // number of write buffers
    if (configs.containsKey(MAX_WRITE_BUFFERS)) {
      options
          .setMaxWriteBufferNumber(Integer.valueOf(String.valueOf(configs.get(MAX_WRITE_BUFFERS))));
    }

    // Size per write buffer
    if (configs.containsKey(WRITE_BUFFER_SIZE)) {
      options.setWriteBufferSize(Long.valueOf(String.valueOf(configs.get(WRITE_BUFFER_SIZE))));
    }

    // ######### Index and filter blocks cache #########
    tableConfig.setCacheIndexAndFilterBlocks(true);
    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    tableConfig.setPinTopLevelIndexAndFilter(true);

    options.setWriteBufferManager(cacheWrapper.writeBufferManager);

    tableConfig.setBlockCache(cacheWrapper.cache);
    options.setTableFormatConfig(tableConfig);
  }

  @VisibleForTesting
  protected void testDestroy() {
    for (CacheWrapper cacheWrapper : cacheConfigPerApplicationId.values()) {
      if (cacheWrapper.writeBufferManager != null) {
        cacheWrapper.writeBufferManager.close();
        cacheWrapper.writeBufferManager = null;
      }
      if (cacheWrapper.cache != null) {
        cacheWrapper.cache.close();
        cacheWrapper.cache = null;
      }
    }
  }
}
