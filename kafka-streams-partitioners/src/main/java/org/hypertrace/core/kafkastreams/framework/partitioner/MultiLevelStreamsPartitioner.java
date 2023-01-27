package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.MultiLevelPartitionerConfig.PartitionGroupConfig;

/**
 * Example config:
 *
 * <pre>
 * mlp.groups.group1.members = tenant-1 # mandatory - for each configured group
 * mlp.groups.group1.weight = 25
 * mlp.groups.group2.members = tenant-2, tenant-3
 * mlp.groups.group2.weight = 25
 * mlp.default.group.weight = 50
 * </pre>
 */
@Slf4j
public class MultiLevelStreamsPartitioner<K, V> implements StreamPartitioner<K, V> {

  private final ConfigProvider configProvider;
  private final BiFunction<K, V, String> groupKeyExtractor;
  private final StreamPartitioner<K, V> delegatePartitioner;

  private final AtomicReference<MultiLevelPartitionerConfig> partitionerConfigRef;

  public MultiLevelStreamsPartitioner(
      ConfigProvider configProvider,
      BiFunction<K, V, String> groupKeyExtractor,
      StreamPartitioner<K, V> delegatePartitioner) {
    this.configProvider = configProvider;
    this.groupKeyExtractor = groupKeyExtractor;
    this.delegatePartitioner = delegatePartitioner;
    this.partitionerConfigRef = new AtomicReference<>(configProvider.getConfig());
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    String groupKey = getGroupKey(key, value);

    PartitionGroupConfig groupConfig = this.getPartitionGroupConfig(groupKey);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    int partition =
        fromIndex
            + (Math.abs(
                    this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
                % numPartitionsForGroup);
    log.debug(
        "fromIndex: {}, toIndex: {}, numGroupPartitions: {}, delegate partition: {}",
        fromIndex,
        toIndex,
        numPartitionsForGroup,
        this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup));
    log.debug("key: {}, value: {}, groupKey: {}, partition:{}", key, value, groupKey, partition);
    return partition;
  }

  private String getGroupKey(K key, V value) {
    return Optional.ofNullable(groupKeyExtractor.apply(key, value)).orElse("");
  }

  private PartitionGroupConfig getPartitionGroupConfig(String partitionKey) {
    return this.partitionerConfigRef
        .get()
        .getGroupConfigByMember()
        .getOrDefault(partitionKey, this.partitionerConfigRef.get().getDefaultGroupConfig());
  }
}
