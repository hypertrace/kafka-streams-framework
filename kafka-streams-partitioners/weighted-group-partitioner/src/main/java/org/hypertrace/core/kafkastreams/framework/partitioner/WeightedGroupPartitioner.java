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
  // Will be used when delegate partitioner returns null
  private final RoundRobinPartitioner<K, V> fallbackDelegatePartitioner;

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
    this.fallbackDelegatePartitioner = new RoundRobinPartitioner<>();
  }

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    WeightedGroup groupConfig = this.getGroupConfig(topic, key, value);
    int fromIndex = (int) Math.floor(groupConfig.getNormalizedFractionalStart() * numPartitions);
    int toIndex = (int) Math.ceil(groupConfig.getNormalizedFractionalEnd() * numPartitions);
    int numPartitionsForGroup = toIndex - fromIndex;

    // partitioner by contract can return null.
    // Refer api doc:  org.apache.kafka.streams.processor.StreamPartitioner.partition
    // when delegate partitioner returns null, we use fallback partitioner (round-robin within
    // group)
    return fromIndex
        + Optional.ofNullable(
                delegatePartitioner.partition(topic, key, value, numPartitionsForGroup))
            .orElse(
                fallbackDelegatePartitioner.partition(topic, key, value, numPartitionsForGroup));
  }

  private WeightedGroup getGroupConfig(String topic, K key, V value) {
    // extractor can return null group key, don't want to fail the whole application in such case.
    // Instead, treat it as default group.
    Optional<String> memberId = Optional.ofNullable(memberIdExtractor.apply(key, value));
    if (memberId.isEmpty()) {
      log.warn("member id is null. profile: {}, topic: {}", profileName, topic);
    }

    return memberId
        .map(id -> this.configServiceClient.getConfig(profileName).getGroupByMember(id))
        .orElseGet(() -> this.configServiceClient.getConfig(profileName).getDefaultGroup());
  }
}
