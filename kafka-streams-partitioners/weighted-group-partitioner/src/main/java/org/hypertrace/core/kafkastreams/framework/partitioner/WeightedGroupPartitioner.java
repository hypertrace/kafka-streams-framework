package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.WeightedGroupProfile.WeightedGroup;

@Slf4j
public class WeightedGroupPartitioner<K, V> implements StreamPartitioner<K, V> {
  private final String profileName;
  private final PartitionerConfigServiceClient configServiceClient;
  private final BiFunction<K, V, String> memberIdExtractor;
  private final StreamPartitioner<K, V> delegatePartitioner;

  public WeightedGroupPartitioner(
      @Nonnull PartitionerConfigServiceClient configServiceClient,
      @Nonnull String profileName,
      @Nonnull BiFunction<K, V, String> memberIdExtractor,
      @Nonnull StreamPartitioner<K, V> delegatePartitioner) {

    Preconditions.checkNotNull(configServiceClient);
    Preconditions.checkNotNull(profileName);
    Preconditions.checkNotNull(memberIdExtractor);
    Preconditions.checkNotNull(delegatePartitioner);

    this.configServiceClient = configServiceClient;
    this.profileName = profileName;
    this.memberIdExtractor = memberIdExtractor;
    this.delegatePartitioner = delegatePartitioner;
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    String memberId = getMemberId(topic, key, value);

    WeightedGroup groupConfig = this.getPartitionGroupInfo(memberId);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    // partitioner by contract can return null.
    // Refer api doc:  org.apache.kafka.streams.processor.StreamPartitioner.partition
    // when delegate partitioner returns null, we treat it as 0
    return fromIndex
        + Optional.ofNullable(
                this.delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
            .orElse(0);
  }

  private String getMemberId(String topic, K key, V value) {
    // If in case extractor returns null, we assign it an empty string which gets resolved to
    // default group.
    // don't want to fail the whole application in such case. Instead, treat it as default group.
    String memberId = memberIdExtractor.apply(key, value);
    if (memberId == null) {
      log.warn(
          "member id is null. using default group. profile: {}, topic: {}", profileName, topic);
      return "";
    }
    return memberId;
  }

  private WeightedGroup getPartitionGroupInfo(String memberId) {
    return this.configServiceClient.getConfig(profileName).getGroupByMember(memberId);
  }
}
