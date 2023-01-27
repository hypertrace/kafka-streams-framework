package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.Map;

public class MapConfigProvider implements ConfigProvider {
  private final Map<String, String> configMap;

  public MapConfigProvider(Map<String, String> configMap) {
    this.configMap = configMap;
  }

  @Override
  public MultiLevelPartitionerConfig getConfig() {
    // parse config from map
    return new MultiLevelPartitionerConfig(configMap);
  }
}
