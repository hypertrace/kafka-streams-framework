package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.hypertrace.core.kafkastreams.framework.partitioner.AvroFieldValuePartitionerConfig.PartitionGroupConfig;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
public class AvroFieldValuePartitioner implements Partitioner {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFieldValuePartitioner.class);
  private static final RateLimiter LOG_RATE_LIMIER = RateLimiter.create(1 / 60.0);

  private AvroFieldValuePartitionerConfig partionerConfig;
  private Table<String, AvroFieldValuePartitionerConfig.PartitionGroupConfig, Iterator<Integer>> parititonIteratorByTopicAndGroup;
  private Partitioner defaultPartitioner;

  @Override
  public void configure(Map<String, ?> configs) {
    this.partionerConfig = new AvroFieldValuePartitionerConfig();
    this.partionerConfig.configure(configs);
    this.parititonIteratorByTopicAndGroup = HashBasedTable.create();

    this.defaultPartitioner = new DefaultPartitioner();
    this.defaultPartitioner.configure(configs);
  }

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    // fallback to default partitioner when topic is not configured or value type is not generic
    // records
    if (!partionerConfig.isTopicConfigured(topic) || !(value instanceof GenericRecord)) {
      if (LOG_RATE_LIMIER.tryAcquire()) {
        LOG.warn(
            "falling back to default partitioner for topic: {}, value type: {}",
            topic,
            value != null ? value.getClass() : "null");
      }
      return defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }
    String partitionKey = this.getKeyFromRecord(topic, (GenericRecord) value).orElse("");
    return this.calculatePartition(cluster, topic, partitionKey);
  }

  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    if (!partionerConfig.isTopicConfigured(topic)) {
      defaultPartitioner.onNewBatch(topic, cluster, prevPartition);
    }
  }

  @Override
  public void close() {}

  private Optional<String> getKeyFromRecord(String topic, GenericRecord record) {
    return Optional.ofNullable(partionerConfig.getFieldNameByTopic().get(topic))
        .filter(record::hasField)
        .map(record::get)
        .map(Object::toString);
  }

  private int calculatePartition(Cluster cluster, String topic, String key) {
    PartitionGroupConfig groupConfig = this.getPartitionGroup(key);
    if (!this.parititonIteratorByTopicAndGroup.contains(topic, groupConfig)) {
      List<Integer> availableTopicPartitions = this.getAvailablePartitionsForTopic(cluster, topic);
      int totalPartitions = availableTopicPartitions.size();
      int fromIndex = (int) (groupConfig.getNormalizedFractionalStart() * totalPartitions);
      int toIndex = (int) (groupConfig.getNormalizedFractionalEnd() * totalPartitions);
      List<Integer> assignedPartitions = availableTopicPartitions.subList(fromIndex, toIndex);
      LOG.info(
          "topic: {}, group config: {}, member: {}, available partitions:{}, assigned partitions: {}",
          topic,
          groupConfig,
          key,
          availableTopicPartitions,
          assignedPartitions);
      // Using cyclic iterator
      Iterator<Integer> partitionIterator = Iterables.cycle(assignedPartitions).iterator();
      this.parititonIteratorByTopicAndGroup.put(topic, groupConfig, partitionIterator);
    }

    return Objects.requireNonNull(this.parititonIteratorByTopicAndGroup.get(topic, groupConfig))
        .next();
  }

  private PartitionGroupConfig getPartitionGroup(String key) {
    return this.partionerConfig
        .getGroupConfigByMember()
        .getOrDefault(key, this.partionerConfig.getDefaultGroupConfig());
  }

  private List<Integer> getAvailablePartitionsForTopic(Cluster cluster, String topic) {
    int totalPartitionCount = Optional.ofNullable(cluster.partitionCountForTopic(topic)).orElse(0);
    List<Integer> availablePartitions = Lists.newArrayList();
    for (int i = 0; i < totalPartitionCount; i++) {
      if (!partionerConfig.getExcludedPartitionsByTopic().get(topic).contains(i)) {
        availablePartitions.add(i);
      }
    }
    return availablePartitions;
  }
}
