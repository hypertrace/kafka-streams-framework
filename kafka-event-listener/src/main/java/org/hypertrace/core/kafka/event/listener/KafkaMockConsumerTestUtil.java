package org.hypertrace.core.kafka.event.listener;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaMockConsumerTestUtil<K, V> {
  private final String topicName;
  private final Map<TopicPartition, Long> currentOffsets;
  @Getter private final MockConsumer<K, V> mockConsumer;

  public KafkaMockConsumerTestUtil(String topicName, int numPartitions) {
    this.topicName = topicName;
    mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    List<PartitionInfo> partitionInfos =
        IntStream.range(0, numPartitions)
            .mapToObj(i -> getPartitionInfo(topicName, i))
            .collect(Collectors.toUnmodifiableList());
    mockConsumer.updatePartitions(topicName, partitionInfos);
    currentOffsets =
        IntStream.range(0, numPartitions)
            .mapToObj(i -> getTopicPartition(topicName, i))
            .collect(Collectors.toMap(Function.identity(), k -> 1L));
    mockConsumer.updateEndOffsets(currentOffsets);
  }

  public void addRecord(K key, V value, int partition) {
    Long latestOffset =
        currentOffsets.computeIfPresent(getTopicPartition(topicName, partition), (k, v) -> v + 1);
    if (Objects.isNull(latestOffset)) {
      throw new UnsupportedOperationException(
          "cannot add to partition "
              + partition
              + ", total partitions is "
              + currentOffsets.size());
    }
    mockConsumer.addRecord(new ConsumerRecord<>(topicName, partition, latestOffset, key, value));
  }

  private static PartitionInfo getPartitionInfo(String topic, int partition) {
    return new PartitionInfo(topic, partition, Node.noNode(), new Node[0], new Node[0]);
  }

  private static TopicPartition getTopicPartition(String topic, int partition) {
    return new TopicPartition(topic, partition);
  }
}
