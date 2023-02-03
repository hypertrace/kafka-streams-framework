package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.MultiLevelPartitionerConfig.PartitionGroupInfo;

@Slf4j
public class MultiLevelStreamPartitioner<K, V> implements StreamPartitioner<K, V> {
  private final ConfigProvider configProvider;
  private final BiFunction<K, V, String> memberIdExtractor;
  private final StreamPartitioner<K, V> delegatePartitioner;

  public MultiLevelStreamPartitioner(
      @Nonnull ConfigProvider configProvider,
      @Nonnull BiFunction<K, V, String> memberIdExtractor,
      @Nullable StreamPartitioner<K, V> delegatePartitioner) {
    this.configProvider = configProvider;
    this.memberIdExtractor = memberIdExtractor;
    this.delegatePartitioner =
        Optional.ofNullable(delegatePartitioner).orElse(new ValueHashPartitioner<>());
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    String memberId = getMemberId(key, value);

    PartitionGroupInfo groupConfig = this.getPartitionGroupInfo(memberId);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    return fromIndex
        + (Utils.toPositive(
                this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
            % numPartitionsForGroup);
  }

  private String getMemberId(K key, V value) {
    // If in case extractor returns null, we assign it an empty string which gets evaluated to
    // default group.
    // don't want to fail the whole application in such case. Instead, treat it as default group.
    return Optional.ofNullable(memberIdExtractor.apply(key, value)).orElse("");
  }

  private PartitionGroupInfo getPartitionGroupInfo(String memberId) {
    return this.configProvider.getConfig().getGroupInfoByMember(memberId);
  }
}
