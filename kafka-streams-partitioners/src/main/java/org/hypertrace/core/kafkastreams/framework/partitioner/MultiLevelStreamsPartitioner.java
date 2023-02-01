package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.Optional;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.MultiLevelPartitionerConfig.PartitionGroupConfig;

/**
 * Example config:
 *
 * <pre>
 * groups.group1.members = tenant-1 # mandatory - for each configured group
 * groups.group1.weight = 25
 * groups.group2.members = tenant-2, tenant-3
 * groups.group2.weight = 25
 * default.group.weight = 50
 * </pre>
 */
@Slf4j
public class MultiLevelStreamsPartitioner<K, V> implements StreamPartitioner<K, V> {

  private final ConfigProvider configProvider;
  private final BiFunction<K, V, String> groupKeyExtractor;
  private final StreamPartitioner<K, V> delegatePartitioner;

  public MultiLevelStreamsPartitioner(
      ConfigProvider configProvider,
      BiFunction<K, V, String> groupKeyExtractor,
      StreamPartitioner<K, V> delegatePartitioner) {
    this.configProvider = configProvider;
    this.groupKeyExtractor = groupKeyExtractor;
    this.delegatePartitioner = delegatePartitioner;
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    String groupKey = getGroupKey(key, value);

    PartitionGroupConfig groupConfig = this.getPartitionGroupConfig(groupKey);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    return fromIndex
        + (Math.abs(this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
            % numPartitionsForGroup);
  }

  private String getGroupKey(K key, V value) {
    return Optional.ofNullable(groupKeyExtractor.apply(key, value)).orElse("");
  }

  private PartitionGroupConfig getPartitionGroupConfig(String partitionKey) {
    MultiLevelPartitionerConfig config = this.configProvider.getConfig();
    return config.getGroupConfigByMember(partitionKey);
  }
}
