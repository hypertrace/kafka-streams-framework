package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.MultiLevelPartitionerConfig.PartitionGroupInfo;

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
public class MultiLevelStreamPartitioner<K, V> implements StreamPartitioner<K, V> {
  private final ConfigProvider configProvider;
  private final BiFunction<K, V, String> groupKeyExtractor;
  private final StreamPartitioner<K, V> delegatePartitioner;

  public MultiLevelStreamPartitioner(
      @Nonnull ConfigProvider configProvider,
      @Nonnull BiFunction<K, V, String> groupKeyExtractor,
      @Nullable StreamPartitioner<K, V> delegatePartitioner) {
    this.configProvider = configProvider;
    this.groupKeyExtractor = groupKeyExtractor;
    this.delegatePartitioner =
        Optional.ofNullable(delegatePartitioner).orElse(new ValueHashPartitioner<>());
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    String groupKey = getGroupKey(key, value);

    PartitionGroupInfo groupConfig = this.getPartitionGroupConfig(groupKey);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    return fromIndex
        + (Utils.toPositive(
                this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
            % numPartitionsForGroup);
  }

  private String getGroupKey(K key, V value) {
    return Optional.ofNullable(groupKeyExtractor.apply(key, value)).orElse("");
  }

  private PartitionGroupInfo getPartitionGroupConfig(String partitionKey) {
    MultiLevelPartitionerConfig config = this.configProvider.getConfig();
    return config.getGroupInfoByMember(partitionKey);
  }
}
