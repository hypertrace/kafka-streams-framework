package org.hypertrace.core.kafkastreams.framework.partitioner;

interface PartitionerConfigServiceClient {
  WeightedGroupProfile getConfig(String profileName);
}
