package org.hypertrace.core.kafkastreams.framework.rocksdb;

import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBCacheProvider.APPLICATION_ID;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.BLOCK_SIZE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_HIGH_PRIORITY_POOL_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.CACHE_WRITE_BUFFERS_RATIO;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPACTION_STYLE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPRESSION_TYPE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DIRECT_READS_ENABLED;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.LOG_LEVEL_CONFIG;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.MAX_SIZE_AMPLIFICATION_PERCENT;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.MAX_WRITE_BUFFERS;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.PERIODIC_COMPACTION_SECONDS;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

class BoundedMemoryConfigSetterTest {

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
    configSetter = new BoundedMemoryConfigSetter();
  }

  @AfterEach
  public void tearDown() {
    RocksDBCacheProvider.get().testDestroy();
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigBlockSize(Map<String, Object> config) {
    configs.put(APPLICATION_ID, config.get(APPLICATION_ID));
    configs.put(BLOCK_SIZE, config.get(BLOCK_SIZE));
    configSetter.setConfig(storeName, options, configs);
    assertEquals(
        ((BlockBasedTableConfig) options.tableFormatConfig()).blockSize(), config.get(BLOCK_SIZE));
    configSetter.close(storeName, options);
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigWriteBufferSize(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.writeBufferSize(), config.get(WRITE_BUFFER_SIZE));
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigCompressionType(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.compressionType(), CompressionType.SNAPPY_COMPRESSION);
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigCompactionStyle(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(
        options.compactionStyle(), CompactionStyle.valueOf((String) configs.get(COMPACTION_STYLE)));
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigLogLevel(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(
        options.infoLogLevel(), InfoLogLevel.valueOf((String) configs.get(LOG_LEVEL_CONFIG)));
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigMaxWriteBuffer(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.maxWriteBufferNumber(), config.get(MAX_WRITE_BUFFERS));
  }

  @ParameterizedTest
  @MethodSource("configProvider")
  public void testSetConfigUseDirectReads(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(options.useDirectReads(), config.get(DIRECT_READS_ENABLED));
  }

  @ParameterizedTest
  @MethodSource("invalidCacheRatioProvider")
  public void testInvalidCacheRatioConfigs(Map<String, Object> invalidConfigs) {
    assertThrows(
        ConfigException.class,
        () -> {
          configs.putAll(invalidConfigs);
          configSetter.setConfig(storeName, options, configs);
          assertEquals(options.useDirectReads(), true);
        });
  }

  @ParameterizedTest
  @MethodSource("universalCompactionConfigProvider")
  public void testSetUniversalConfigOptions(Map<String, Object> config) {
    configs.putAll(config);
    configSetter.setConfig(storeName, options, configs);
    assertEquals(
        options.periodicCompactionSeconds(),
        Long.valueOf(String.valueOf(configs.get(PERIODIC_COMPACTION_SECONDS))));
    assertEquals(
        options.compactionOptionsUniversal().maxSizeAmplificationPercent(),
        configs.get(MAX_SIZE_AMPLIFICATION_PERCENT));
  }

  // Data provider for negative tests
  static Stream<Map<String, Object>> invalidCacheRatioProvider() {
    return Stream.of(
        Map.of(CACHE_WRITE_BUFFERS_RATIO, -0.1, APPLICATION_ID, "app-1"),
        Map.of(CACHE_WRITE_BUFFERS_RATIO, 1.1, APPLICATION_ID, "app-2"),
        Map.of(CACHE_HIGH_PRIORITY_POOL_RATIO, -0.1, APPLICATION_ID, "app-3"),
        Map.of(CACHE_HIGH_PRIORITY_POOL_RATIO, 1.1, APPLICATION_ID, "app-2"),
        Map.of(
            CACHE_WRITE_BUFFERS_RATIO,
            0.9,
            CACHE_HIGH_PRIORITY_POOL_RATIO,
            0.2,
            APPLICATION_ID,
            "app-5"));
  }

  static Stream<Map<String, Object>> configProvider() {
    return Stream.of(
        Map.of(
            APPLICATION_ID,
            "app-1",
            BLOCK_SIZE,
            8388608L,
            WRITE_BUFFER_SIZE,
            8388608L,
            COMPRESSION_TYPE,
            "SNAPPY_COMPRESSION",
            COMPACTION_STYLE,
            CompactionStyle.LEVEL.toString(),
            LOG_LEVEL_CONFIG,
            InfoLogLevel.INFO_LEVEL.toString(),
            MAX_WRITE_BUFFERS,
            2,
            DIRECT_READS_ENABLED,
            true,
            PERIODIC_COMPACTION_SECONDS,
            60,
            MAX_SIZE_AMPLIFICATION_PERCENT,
            50),
        Map.of(
            APPLICATION_ID,
            "app-2",
            BLOCK_SIZE,
            8388609L,
            WRITE_BUFFER_SIZE,
            8388607L,
            COMPRESSION_TYPE,
            "SNAPPY_COMPRESSION",
            COMPACTION_STYLE,
            CompactionStyle.UNIVERSAL.toString(),
            LOG_LEVEL_CONFIG,
            InfoLogLevel.DEBUG_LEVEL.toString(),
            MAX_WRITE_BUFFERS,
            3,
            DIRECT_READS_ENABLED,
            true,
            PERIODIC_COMPACTION_SECONDS,
            60,
            MAX_SIZE_AMPLIFICATION_PERCENT,
            50),
        Map.of(
            APPLICATION_ID,
            "app-3",
            BLOCK_SIZE,
            8388607L,
            WRITE_BUFFER_SIZE,
            8388609L,
            COMPRESSION_TYPE,
            "SNAPPY_COMPRESSION",
            COMPACTION_STYLE,
            CompactionStyle.FIFO.toString(),
            LOG_LEVEL_CONFIG,
            InfoLogLevel.ERROR_LEVEL.toString(),
            MAX_WRITE_BUFFERS,
            4,
            DIRECT_READS_ENABLED,
            false,
            PERIODIC_COMPACTION_SECONDS,
            60,
            MAX_SIZE_AMPLIFICATION_PERCENT,
            50));
  }

  static Stream<Map<String, Object>> universalCompactionConfigProvider() {
    return Stream.of(
        Map.of(
            APPLICATION_ID,
            "app-2",
            COMPACTION_STYLE,
            CompactionStyle.UNIVERSAL.toString(),
            PERIODIC_COMPACTION_SECONDS,
            60,
            MAX_SIZE_AMPLIFICATION_PERCENT,
            50));
  }
}
