package org.hypertrace.core.kafkastreams.framework.partitioner;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AvroFieldValuePartitionerTest {
  private static final String TOPIC_A = "TOPIC_A";
  private static final String TOPIC_B = "TOPIC_B";
  private Cluster testCluster;
  private Node[] nodes;
  private Iterator<Node> nodeCycle;

  @BeforeEach
  public void setUp() {
    nodes =
        new Node[] {
          new Node(0, "localhost", 99), new Node(1, "localhost", 100), new Node(2, "localhost", 101)
        };

    nodeCycle = Iterables.cycle(nodes).iterator();
    List<PartitionInfo> allPartitions = new ArrayList<>();
    allPartitions.addAll(createTopic(TOPIC_A, 8));
    allPartitions.addAll(createTopic(TOPIC_B, 16));
    testCluster =
        new Cluster(
            "test-cluster",
            asList(nodes[0], nodes[1], nodes[2]),
            allPartitions,
            Collections.emptySet(),
            Collections.emptySet());
  }

  @Test
  public void testWithSingleTopic() {
    // TOPIC_A: total partitions:8, excluded:{4,5,6,7}, available:{0,1,2,3}
    // default group: weight=25%,  parts:{0}
    // group1: members:{tenant1}, weight=50%, parts:{1,2}
    // group2: members:{tenant2,tenant3}, weight=25%, parts:{3}
    Map<String, String> streamConfigs = Maps.newHashMap();

    streamConfigs.put("avro.field.value.partitioner.topics.TOPIC_A.field.name", "customer_id");
    streamConfigs.put("avro.field.value.partitioner.topics.TOPIC_A.excluded.partitions", "4,5,6,7");
    streamConfigs.put("avro.field.value.partitioner.groups.group1.members", "tenant-1");
    streamConfigs.put("avro.field.value.partitioner.groups.group1.weight", "50");
    streamConfigs.put("avro.field.value.partitioner.groups.group2.members", "tenant-2, tenant-3");
    streamConfigs.put("avro.field.value.partitioner.groups.group2.weight", "25");
    streamConfigs.put("avro.field.value.partitioner.default.group.weight", "25");

    AvroFieldValuePartitioner<GenericRecord> partitioner =
        new AvroFieldValuePartitioner<>(streamConfigs);
    Map<Integer, Integer> topicPartitionCounter;

    // `tenant-1` weight = 50% i.e, 1,2 partitions are assigned to tenant-1
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-1",
            20);
    assertEquals(2, topicPartitionCounter.size());
    assertEquals(10, topicPartitionCounter.get(1));
    assertEquals(10, topicPartitionCounter.get(2));

    // `tenant-2` weight = 25% i.e, 1,2 partitions are assigned to tenant-2
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-2",
            20);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(20, topicPartitionCounter.get(3));

    // `tenant-3` weight = 25% i.e, 1,2 partitions are assigned to tenant-3
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-3",
            20);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(20, topicPartitionCounter.get(3));

    // `default` weight = 25% i.e, 0th partition is assigned to all other tenants
    // send 50 messages - expecting all 50 from 0th partition
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "others",
            50);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(50, topicPartitionCounter.get(0));
  }

  @Test
  public void testWithMultipleTopics() {
    Map<String, String> streamConfigs = Maps.newHashMap();

    // TOPIC_A: total partitions:8, excluded:{4,5,6,7}, available:{0,1,2,3}
    // default group: weight=25%,  parts:{0}
    // group1: members:{tenant1}, weight=50%, parts:{1,2}
    // group2: members:{tenant2,tenant3}, weight=25%, parts:{3}

    // TOPIC_B: total partitions:16, excluded:{0,5,9,14}, available:{1,2,3,4,6,7,8,10,11,12,13,15}
    // default group: weight=25%,  parts:{1,2,3}
    // group1: members:{tenant1}, weight=50%, parts:{4,6,7,8,10,11}
    // group2: members:{tenant2}, weight=25%, parts:{12,13,15}
    streamConfigs.put("avro.field.value.partitioner.topics.TOPIC_A.field.name", "customer_id");
    streamConfigs.put("avro.field.value.partitioner.topics.TOPIC_A.excluded.partitions", "4,5,6,7");
    streamConfigs.put("avro.field.value.partitioner.topics.TOPIC_B.field.name", "tenant_id");
    streamConfigs.put(
        "avro.field.value.partitioner.topics.TOPIC_B.excluded.partitions", "0, 5, 9, 14");
    streamConfigs.put("avro.field.value.partitioner.groups.group1.members", "tenant-1");
    streamConfigs.put("avro.field.value.partitioner.groups.group1.weight", "50");
    streamConfigs.put("avro.field.value.partitioner.groups.group2.members", "tenant-2, tenant-3");
    streamConfigs.put("avro.field.value.partitioner.groups.group2.weight", "25");
    streamConfigs.put("avro.field.value.partitioner.default.group.weight", "25");

    AvroFieldValuePartitioner<GenericRecord> partitioner =
        new AvroFieldValuePartitioner<>(streamConfigs);
    Map<Integer, Integer> topicPartitionCounter;

    // ###############
    // TOPIC_A
    // ###############
    // `tenant-1` weight = 50% i.e, 1,2 partitions are assigned to tenant-1
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-1",
            20);
    assertEquals(2, topicPartitionCounter.size());
    assertEquals(10, topicPartitionCounter.get(1));
    assertEquals(10, topicPartitionCounter.get(2));

    // `tenant-2` weight = 25% i.e, 1,2 partitions are assigned to tenant-2
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-2",
            20);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(20, topicPartitionCounter.get(3));

    // `tenant-3` weight = 25% i.e, 1,2 partitions are assigned to tenant-3
    // send 20 messages - expecting all 20 on partition# 3
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "tenant-3",
            20);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(20, topicPartitionCounter.get(3));

    // `default` weight = 25% i.e, 0th partition is assigned to all other tenants
    // send 50 messages - expecting all 50 from 0th partition
    topicPartitionCounter =
        produce(
            testCluster,
            TOPIC_A,
            partitioner,
            TestCustomerRecord::new,
            "customer_id",
            "others",
            50);
    assertEquals(1, topicPartitionCounter.size());
    assertEquals(50, topicPartitionCounter.get(0));

    // ###############
    // TOPIC_B
    // ###############
    // `tenant-1` weight = 50% i.e, {4,6,7,8,10,11} partitions are assigned to tenant-1
    // send 60 messages - expecting 10 on each assigned partition
    topicPartitionCounter =
        produce(
            testCluster, TOPIC_B, partitioner, TestTenantRecord::new, "tenant_id", "tenant-1", 60);
    assertEquals(6, topicPartitionCounter.size());
    verifyPartitionCounts(topicPartitionCounter, new Integer[] {4, 6, 7, 8, 10, 11}, 10);

    // `tenant-2` weight = 25% i.e, {12,13,15} partitions are assigned to tenant-2
    // send 30 messages - expecting 10 on each assigned partition
    topicPartitionCounter =
        produce(
            testCluster, TOPIC_B, partitioner, TestTenantRecord::new, "tenant_id", "tenant-2", 30);
    assertEquals(3, topicPartitionCounter.size());
    verifyPartitionCounts(topicPartitionCounter, new Integer[] {12, 13, 15}, 10);

    // `tenant-3` weight = 25% i.e, {12,13,15} partitions are assigned to tenant-3
    // send 30 messages - expecting 10 on each assigned partition
    topicPartitionCounter =
        produce(
            testCluster, TOPIC_B, partitioner, TestTenantRecord::new, "tenant_id", "tenant-3", 30);
    assertEquals(3, topicPartitionCounter.size());
    verifyPartitionCounts(topicPartitionCounter, new Integer[] {12, 13, 15}, 10);

    // `default` weight = 25% i.e, {1,2,3} partitions are assigned to all other tenants
    // send 60 messages - expecting 20 on each assigned partition
    topicPartitionCounter =
        produce(
            testCluster, TOPIC_B, partitioner, TestTenantRecord::new, "tenant_id", "others", 60);
    assertEquals(3, topicPartitionCounter.size());
    verifyPartitionCounts(topicPartitionCounter, new Integer[] {1, 2, 3}, 20);
  }

  private void verifyPartitionCounts(
      final Map<Integer, Integer> topicPartitionCounter, Integer[] partitons, int expectedCount) {
    Arrays.stream(partitons)
        .forEach(part -> assertEquals(expectedCount, topicPartitionCounter.get(part)));
  }

  @Test
  public void testConfigParsing() {
    Map<String, Object> properties = Maps.newHashMap();
    properties.put("avro.field.value.partitioner.topics.topic1.field.name", "customer_id");
    properties.put("avro.field.value.partitioner.topics.topic1.excluded.partitions", "4,5,6,7");
    properties.put("avro.field.value.partitioner.topics.topic2.field.name", "tenant_id");
    properties.put("avro.field.value.partitioner.topics.topic2.excluded.partitions", "12,13,14,15");
    properties.put("avro.field.value.partitioner.groups.group1.members", "tenant-1");
    properties.put("avro.field.value.partitioner.groups.group1.weight", "25");
    properties.put("avro.field.value.partitioner.groups.group2.members", "tenant-2, tenant-3");
    properties.put("avro.field.value.partitioner.groups.group2.weight", "25");
    properties.put("avro.field.value.partitioner.default.group.weight", "50");

    // we get other configs as well
    properties.put("other.stream.config.key", "value");
    properties.put("jobConfig", ConfigFactory.empty());

    AvroFieldValuePartitionerConfig config = new AvroFieldValuePartitionerConfig(properties);

    assertEquals(2, config.getExcludedPartitionsByTopic().size());
    assertEquals("customer_id", config.getFieldNameByTopic().get("topic1"));
    assertEquals("tenant_id", config.getFieldNameByTopic().get("topic2"));
    assertEquals(
        Sets.newTreeSet(Lists.newArrayList(4, 5, 6, 7)),
        config.getExcludedPartitionsByTopic().get("topic1"));
    assertEquals(
        Sets.newTreeSet(Lists.newArrayList(12, 13, 14, 15)),
        config.getExcludedPartitionsByTopic().get("topic2"));
  }

  @Test
  public void testWithMinimalConfig() {
    Map<String, Object> properties = Maps.newHashMap();
    properties.put("avro.field.value.partitioner.topics.topic1.field.name", "customer_id");
    properties.put("avro.field.value.partitioner.topics.topic2.field.name", "tenant_id");
    properties.put("avro.field.value.partitioner.default.group.weight", "50");

    AvroFieldValuePartitionerConfig config = new AvroFieldValuePartitionerConfig(properties);

    assertEquals(2, config.getExcludedPartitionsByTopic().size());
    assertEquals("customer_id", config.getFieldNameByTopic().get("topic1"));
    assertEquals("tenant_id", config.getFieldNameByTopic().get("topic2"));
    assertTrue(config.getExcludedPartitionsByTopic().get("topic1").isEmpty());
    assertTrue(config.getExcludedPartitionsByTopic().get("topic2").isEmpty());
  }

  private Map<Integer, Integer> produce(
      Cluster testCluster,
      String topic,
      AvroFieldValuePartitioner<GenericRecord> partitioner,
      Supplier<GenericRecord> genericRecordSupplier,
      String fieldName,
      String fieldValue,
      int msgCount) {
    Map<Integer, Integer> topicPartitionCounter = Maps.newHashMap();
    int topicPartition;

    for (int i = 0; i < msgCount; ++i) {
      GenericRecord record = newRecord(genericRecordSupplier, fieldName, fieldValue);
      topicPartition =
          partitioner.partition(topic, null, record, testCluster.partitionCountForTopic(topic));
      Integer topicPartitionCount = topicPartitionCounter.get(topicPartition);
      if (null == topicPartitionCount) topicPartitionCount = 0;
      topicPartitionCounter.put(topicPartition, topicPartitionCount + 1);
    }

    return topicPartitionCounter;
  }

  private GenericRecord newRecord(Supplier<GenericRecord> supplier, String... kvPairs) {
    GenericRecord record = supplier.get();
    for (int i = 0; i < kvPairs.length; i += 2) {
      record.put(kvPairs[i], kvPairs[i + 1]);
    }
    return record;
  }

  private List<PartitionInfo> createTopic(String topic, int numPartitions) {
    List<PartitionInfo> partitionInfos = Lists.newArrayList();
    for (int i = 0; i < numPartitions; i++) {
      Node currentNode = nodeCycle.next();
      partitionInfos.add(new PartitionInfo(topic, i, currentNode, nodes, nodes));
    }
    return partitionInfos;
  }
}
