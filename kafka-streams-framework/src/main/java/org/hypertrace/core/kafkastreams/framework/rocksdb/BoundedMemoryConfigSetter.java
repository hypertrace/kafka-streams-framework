package org.hypertrace.core.kafkastreams.framework.rocksdb;

import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPACTION_STYLE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.COMPRESSION_TYPE;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.DIRECT_READS_ENABLED;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.LOG_LEVEL_CONFIG;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.MAX_SIZE_AMPLIFICATION_PERCENT;
import static org.hypertrace.core.kafkastreams.framework.rocksdb.RocksDBConfigs.OPTIMIZE_FOR_POINT_LOOKUPS;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

public class BoundedMemoryConfigSetter implements RocksDBConfigSetter {

  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {
    // Initialize
    RocksDBCacheProvider.get().initCache(options, configs);

    if (configs.containsKey(COMPACTION_STYLE)) {
      options.setCompactionStyle(CompactionStyle.valueOf((String) configs.get(COMPACTION_STYLE)));
    }

    setUniversalConfigOptions(options, configs);

    if (configs.containsKey(COMPRESSION_TYPE)) {
      options.setCompressionType(
          CompressionType.valueOf(String.valueOf(configs.get(COMPRESSION_TYPE))));
    }

    if (configs.containsKey(LOG_LEVEL_CONFIG)) {
      options.setInfoLogLevel(InfoLogLevel.valueOf((String) configs.get(LOG_LEVEL_CONFIG)));
    }

    if (configs.containsKey(DIRECT_READS_ENABLED)) {
      options.setUseDirectReads(Boolean.valueOf(String.valueOf(configs.get(DIRECT_READS_ENABLED))));
    }

    if (configs.containsKey(OPTIMIZE_FOR_POINT_LOOKUPS)) {
      Boolean optimizeForPointLookups =
          Boolean.valueOf(String.valueOf(configs.get(OPTIMIZE_FOR_POINT_LOOKUPS)));
      if (optimizeForPointLookups) {
        long blockCacheSizeMb =
            ((BlockBasedTableConfig) options.tableFormatConfig()).blockCacheSize()
                / (1024L * 1024L);
        options.optimizeForPointLookup(blockCacheSizeMb);
      }
    }
  }

  private void setUniversalConfigOptions(Options options, Map<String, Object> configs) {
    boolean configured = false;
    CompactionOptionsUniversal compactionOptions = new CompactionOptionsUniversal();

    if (configs.containsKey(MAX_SIZE_AMPLIFICATION_PERCENT)) {
      configured = true;
      compactionOptions.setMaxSizeAmplificationPercent(Integer.parseInt((String) configs.get(MAX_SIZE_AMPLIFICATION_PERCENT)));
    }

    if (configured) {
      options.setCompactionOptionsUniversal(compactionOptions);
    }
  }

  @Override
  public void close(String storeName, Options options) {
    // Do nothing
    // Cache MUST NOT be closed as its shared across all the instances.
  }
}
