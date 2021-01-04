package org.hypertrace.core.kafkastreams.framework.rocksdb;

import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.BLOCK_SIZE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_BLOCK_CACHE_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_HIGH_PRIORITY_POOL_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_INDEX_AND_FILTER_BLOCKS;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_TOTAL_CAPACITY;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_WRITE_BUFFERS_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPACTION_STYLE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPRESSION_TYPE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DEFAULT_CACHE_BLOCK_CACHE_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DIRECT_READS_ENABLED;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.LOG_LEVEL_CONFIG;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.MAX_WRITE_BUFFERS;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.WRITE_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

class RocksDBStateStoreConfigSetterTest {

  private final String storeName = "test-store";
  private Options options;
  private Map<String, Object> configs;
  private RocksDBConfigSetter configSetter;
  private BlockBasedTableConfig tableConfig;

  @BeforeEach
  public void setUp() {
    RocksDBCacheProvider.get().testDestroy();
    options = new Options();
    configs = new HashMap<>();
    tableConfig = new BlockBasedTableConfig();
    options.setTableFormatConfig(tableConfig);
    configSetter = new RocksDBStateStoreConfigSetter();
  }

  @AfterEach
  public void tearDown() {
    RocksDBCacheProvider.get().testDestroy();
  }

  @Test
  public void testSetConfigBlockSize() {
    configs.put(BLOCK_SIZE, 8388608L);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(((BlockBasedTableConfig) options.tableFormatConfig()).blockSize(), 8388608L);
    configSetter.close(storeName, options);
  }

  @Test
  public void testDeafultBlockCache() {
    long totalMemoryCapacity = 64 * 1024 * 1024;
    configs.put(CACHE_TOTAL_CAPACITY, totalMemoryCapacity);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(tableConfig.blockCacheSize(),
        (long) (totalMemoryCapacity * DEFAULT_CACHE_BLOCK_CACHE_RATIO));
  }

  @Test
  public void testSetConfigWriteBufferSize() {
    configs.put(WRITE_BUFFER_SIZE, 8388608L);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.writeBufferSize(), 8388608L);
  }

  @Test
  public void testSetConfigCompressionType() {
    configs.put(COMPRESSION_TYPE, "SNAPPY_COMPRESSION");
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.compressionType(), CompressionType.SNAPPY_COMPRESSION);
  }

  @Test
  public void testSetConfigCompactionStyle() {
    configs.put(COMPACTION_STYLE, "UNIVERSAL");
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.compactionStyle(), CompactionStyle.UNIVERSAL);
  }

  @Test
  public void testSetConfigLogLevel() {
    configs.put(LOG_LEVEL_CONFIG, "INFO_LEVEL");
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.infoLogLevel(), InfoLogLevel.INFO_LEVEL);
  }

  @Test
  public void testSetConfigMaxWriteBuffer() {
    configs.put(MAX_WRITE_BUFFERS, 2);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.maxWriteBufferNumber(), 2);
  }

  @Test
  public void testSetConfigCacheIndexAndFilterBlocks() {
    configs.put(CACHE_INDEX_AND_FILTER_BLOCKS, true);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(((BlockBasedTableConfig) options.tableFormatConfig()).cacheIndexAndFilterBlocks(),
        true);
  }

  @Test
  public void testSetConfigUseDirectReads() {
    configs.put(DIRECT_READS_ENABLED, true);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.useDirectReads(), true);
  }

  @ParameterizedTest
  @MethodSource("invalidCacheRatioProvider")
  public void testInvalidCacheRatioConfigs(Map<String, Object> invalidConfigs) {
    assertThrows(ConfigException.class, () -> {
      configs.putAll(invalidConfigs);
      configSetter.setConfig(storeName, options, invalidConfigs);
      assertEquals(options.useDirectReads(), true);
    });
  }

  // Data provider for negative tests
  static Stream<Map<String, Object>> invalidCacheRatioProvider() {
    return Stream.of(
        Map.of(CACHE_BLOCK_CACHE_RATIO, -0.1),
        Map.of(CACHE_BLOCK_CACHE_RATIO, 1.1),
        Map.of(CACHE_WRITE_BUFFERS_RATIO, -0.1),
        Map.of(CACHE_WRITE_BUFFERS_RATIO, 1.1),
        Map.of(CACHE_HIGH_PRIORITY_POOL_RATIO, -0.1),
        Map.of(CACHE_HIGH_PRIORITY_POOL_RATIO, 1.1),
        Map.of(CACHE_BLOCK_CACHE_RATIO, 0.5, CACHE_WRITE_BUFFERS_RATIO, 0.4,
            CACHE_HIGH_PRIORITY_POOL_RATIO, 0.2)
    );
  }
}
