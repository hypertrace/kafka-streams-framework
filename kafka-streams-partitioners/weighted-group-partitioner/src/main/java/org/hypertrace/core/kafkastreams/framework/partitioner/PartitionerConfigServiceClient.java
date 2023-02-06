package org.hypertrace.core.kafkastreams.framework.partitioner;

public interface PartitionerConfigServiceClient {
  WeightedGroupProfile getConfig(String profileName);
}
