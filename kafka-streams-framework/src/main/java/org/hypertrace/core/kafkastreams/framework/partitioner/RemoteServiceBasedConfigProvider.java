package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.hypertrace.core.kafkastreams.framework.partitioner.MultiLevelPartitionerConfig.PARTITIONER_CONFIG_PREFIX;

import com.typesafe.config.Config;

public class RemoteServiceBasedConfigProvider implements ConfigProvider {
  private final Config partitionerConfig;

  public RemoteServiceBasedConfigProvider(Config streamsConfig) {
    this.partitionerConfig = streamsConfig.withOnlyPath(PARTITIONER_CONFIG_PREFIX);
    // TODO: Fetch config using config service client
    // TODO: refresh interval
  }

  @Override
  public MultiLevelPartitionerConfig getConfig() {
    // TODO fetch the config from the service
    return null;
  }
}
