package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.kafkastreams.framework.partitioner.AvroFieldValuePartitionerConfig.PartitionGroupConfig;

/**
 * Example config:
 *
 * <pre>
 * avro.field.value.partitioner.topics.topic1.field.name = customer_id # mandatory
 * avro.field.value.partitioner.topics.topic1.excluded.partitions = "4,5,6,7" # Optional, default empty set
 * avro.field.value.partitioner.topics.topic2.field.name = tenant_id # mandatory for each configured topic
 * avro.field.value.partitioner.topics.topic2.excluded.partitions = "12,13,14,15" # Optional, default empty set
 *
 * avro.field.value.partitioner.groups.group1.members = tenant-1 # mandatory - for each configured group
 * avro.field.value.partitioner.groups.group1.weight = 25
 * avro.field.value.partitioner.groups.group2.members = tenant-2, tenant-3
 * avro.field.value.partitioner.groups.group2.weight = 25
 * avro.field.value.partitioner.default.group.weight = 50
 * </pre>
 */
@Slf4j
public class AvroFieldValuePartitioner<V extends GenericRecord>
    implements StreamPartitioner<Object, V> {
  private final AvroFieldValuePartitionerConfig partitionerConfig;
  private final Table<
          String, AvroFieldValuePartitionerConfig.PartitionGroupConfig, Iterator<Integer>>
      partitionIteratorByTopicAndGroup = HashBasedTable.create();

  public AvroFieldValuePartitioner(Map<String, ?> configs) {
    this.partitionerConfig = new AvroFieldValuePartitionerConfig(configs);
  }

  @Override
  public Integer partition(String topic, Object ignoredKey, V value, int numPartitions) {
    String partitionKey = this.getPartitionKeyFromRecord(topic, value).orElse("");
    return this.calculatePartition(topic, partitionKey, numPartitions);
  }

  private Optional<String> getPartitionKeyFromRecord(String topic, GenericRecord record) {
    return Optional.ofNullable(partitionerConfig.getFieldNameByTopic().get(topic))
        .filter(record::hasField)
        .map(record::get)
        .map(Object::toString);
  }

  private int calculatePartition(String topic, String key, int numPartitions) {
    PartitionGroupConfig groupConfig = this.getPartitionGroup(key);
    if (!this.partitionIteratorByTopicAndGroup.contains(topic, groupConfig)) {
      List<Integer> availableTopicPartitions =
          this.getAvailablePartitionsForTopic(topic, numPartitions);
      int totalPartitions = availableTopicPartitions.size();
      int fromIndex = (int) (groupConfig.getNormalizedFractionalStart() * totalPartitions);
      int toIndex = (int) (groupConfig.getNormalizedFractionalEnd() * totalPartitions);
      List<Integer> assignedPartitions = availableTopicPartitions.subList(fromIndex, toIndex);
      log.info(
          "topic: {}, group config: {}, member: {}, available partitions:{}, assigned partitions: {}",
          topic,
          groupConfig,
          key,
          availableTopicPartitions,
          assignedPartitions);
      // Using cyclic iterator
      Iterator<Integer> partitionIterator = Iterables.cycle(assignedPartitions).iterator();
      synchronized (partitionIteratorByTopicAndGroup) {
        this.partitionIteratorByTopicAndGroup.put(topic, groupConfig, partitionIterator);
      }
    }

    Iterator<Integer> iterator =
        Objects.requireNonNull(this.partitionIteratorByTopicAndGroup.get(topic, groupConfig));
    synchronized (iterator) {
      return iterator.next();
    }
  }

  private PartitionGroupConfig getPartitionGroup(String key) {
    return this.partitionerConfig
        .getGroupConfigByMember()
        .getOrDefault(key, this.partitionerConfig.getDefaultGroupConfig());
  }

  private List<Integer> getAvailablePartitionsForTopic(String topic, int totalPartitionCount) {
    return IntStream.range(0, totalPartitionCount)
        .boxed()
        .filter(not(partitionerConfig.getExcludedPartitionsByTopic().get(topic)::contains))
        .collect(toUnmodifiableList());
  }
}
