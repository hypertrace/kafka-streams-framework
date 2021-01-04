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

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBCacheProvider.class);

  private static final RocksDBCacheProvider PROVIDER = new RocksDBCacheProvider();

  private Cache cache;
  private WriteBufferManager writeBufferManager;

  private long cacheTotalCapacity;
  private double writeBuffersRatio;
  private double highPriorityPoolRatio;

  /**
   * Singleton
   */
  private RocksDBCacheProvider() {
    // Do nothing - Lazy initialization
  }

  public static RocksDBCacheProvider get() {
    return PROVIDER;
  }

  protected synchronized void initCache(Options options, Map<String, Object> configs) {
    if (cache == null) {
      cacheTotalCapacity = DEFAULT_CACHE_TOTAL_CAPACITY;
      if (configs.containsKey(CACHE_TOTAL_CAPACITY)) {
        cacheTotalCapacity = Long.valueOf(String.valueOf(configs.get(CACHE_TOTAL_CAPACITY)));
      }

      writeBuffersRatio = DEFAULT_CACHE_WRITE_BUFFERS_RATIO;
      if (configs.containsKey(CACHE_WRITE_BUFFERS_RATIO)) {
        writeBuffersRatio = Double.valueOf(String.valueOf(configs.get(CACHE_WRITE_BUFFERS_RATIO)));
      }

      if (writeBuffersRatio < 0.0 || writeBuffersRatio > 1.0) {
        throw new ConfigException(
            "Invalid high priority write buffers ratio configured. Config key: "
                + CACHE_WRITE_BUFFERS_RATIO + ", configured value: " + String.valueOf(
                configs.get(CACHE_WRITE_BUFFERS_RATIO) + ", Allowed value range: (0.0, 1.0)"));
      }

      highPriorityPoolRatio = DEFAULT_CACHE_HIGH_PRIORITY_POOL_RATIO;
      if (configs.containsKey(CACHE_HIGH_PRIORITY_POOL_RATIO)) {
        highPriorityPoolRatio = Double
            .valueOf(String.valueOf(configs.get(CACHE_HIGH_PRIORITY_POOL_RATIO)));
      }

      if (highPriorityPoolRatio < 0.0 || highPriorityPoolRatio > 1.0) {
        throw new ConfigException("Invalid high priority cache pool ratio configured. Config key: "
            + CACHE_HIGH_PRIORITY_POOL_RATIO + ", configured value: " + String.valueOf(
            configs.get(CACHE_HIGH_PRIORITY_POOL_RATIO) + ", Allowed value range: (0.0, 1.0)"));
      }

      double aggregatedRatio = writeBuffersRatio + highPriorityPoolRatio;
      if (aggregatedRatio > 1.0) {
        throw new ConfigException("Sum total of the cache ratios: " + aggregatedRatio
            + " is greater than 1.0."
            + " Configure all the cache ratios appropriately.");
      }

      // Strict capacity limit can't be used due to a bug in RocksDB
      // numShardBits = -1 means it is automatically determined
      // Read https://kafka.apache.org/26/documentation/streams/developer-guide/memory-mgmt.html#id3
      cache = new LRUCache(cacheTotalCapacity, -1, false, highPriorityPoolRatio);

      long writeBuffersTotalCapacity = (long) (writeBuffersRatio * cacheTotalCapacity);
      // makes write buffers are allocated from shared cache
      writeBufferManager = new WriteBufferManager(writeBuffersTotalCapacity, cache);

      LOG.info(
          "RocksDB shared cache initialized successfully. Total cache size(MB): {},"
              + " write buffers ratio: {}, high priority pool ratio: {}",
          cacheTotalCapacity / (1024 * 1024), writeBuffersRatio, highPriorityPoolRatio);
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

    options.setWriteBufferManager(writeBufferManager);

    tableConfig.setBlockCache(cache);
    options.setTableFormatConfig(tableConfig);
  }

  @VisibleForTesting
  protected void testDestroy() {
    if (writeBufferManager != null) {
      writeBufferManager.close();
      writeBufferManager = null;
    }
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }
}
