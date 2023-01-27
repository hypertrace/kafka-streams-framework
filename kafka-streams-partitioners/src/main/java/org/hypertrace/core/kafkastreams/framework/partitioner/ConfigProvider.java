package org.hypertrace.core.kafkastreams.framework.partitioner;

interface ConfigProvider {
  MultiLevelPartitionerConfig getConfig();
}
