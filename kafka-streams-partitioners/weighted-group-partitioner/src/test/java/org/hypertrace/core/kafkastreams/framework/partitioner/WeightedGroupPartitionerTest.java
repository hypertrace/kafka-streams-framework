package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;
import org.junit.jupiter.api.Test;

public class WeightedGroupPartitionerTest {
  private final PartitionerConfigServiceClient configServiceClient = getTestServiceClient();
  private final BiFunction<String, String, String> groupKeyExtractor = (key, value) -> key;
  private final StreamPartitioner<String, String> roundRobinPartitioner =
      new RoundRobinPartitioner<>();
  private final StreamPartitioner<String, String> keyHashPartitioner = new KeyHashPartitioner<>();

  @Test
  public void testPartitionerWithNonOverlappingGroupPartitions() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", configServiceClient, groupKeyExtractor, roundRobinPartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0,1])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 8);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 4: groupKey=unknown should use default group [4,5,6,7]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 8);
    assertTrue(partition >= 4 && partition <= 7);
  }

  @Test
  public void testPartitionerWithOverlappingGroupPartitions() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", configServiceClient, groupKeyExtractor, roundRobinPartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 3);
    assertEquals(0, partition);

    // Test case 2: tenant-2 belong to group-2 (partitions: [0,1])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 3);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 3: tenant-3 belong to group-2 (partitions: [0,1])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 3);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 4: groupKey=unknown should use default group [1,2]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 3);
    assertTrue(partition >= 1 && partition <= 2);
  }

  @Test
  public void testPartitionerWithSinglePartition() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", configServiceClient, groupKeyExtractor, roundRobinPartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 1);
    assertEquals(0, partition);

    // Test case 2: tenant-2 belong to group-2 (partitions: [0])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 1);
    assertEquals(0, partition);

    // Test case 3: tenant-3 belong to group-2 (partitions: [0])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 1);
    assertEquals(0, partition);

    // Test case 4: groupKey=unknown should use default group [0]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 1);
    assertEquals(0, partition);
  }

  @Test
  public void testPartitionerWhenGroupKeyIsNull() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", configServiceClient, (key, value) -> null, roundRobinPartitioner);

    // should always use default group when group key is null [4,5,6,7]
    int partition = partitioner.partition("test-topic", null, "value-1", 8);
    assertEquals(4, partition);

    partition = partitioner.partition("test-topic", null, "value-2", 8);
    assertEquals(5, partition);

    partition = partitioner.partition("test-topic", null, "value-3", 8);
    assertEquals(6, partition);

    partition = partitioner.partition("test-topic", null, "value-4", 8);
    assertEquals(7, partition);

    // round-robin
    partition = partitioner.partition("test-topic", null, "value-5", 8);
    assertEquals(4, partition);
  }

  @Test
  public void testPartitionerWhenDelegateReturnsNull() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans",
            configServiceClient,
            groupKeyExtractor,
            (topic, key, value, numPartitions) -> null);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0,1])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 8);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 4: tenant-2 belong to group-2 (partitions: [2,3]) - should go round-robin when
    // delegate returns null
    partition = partitioner.partition("test-topic", "tenant-2", "span-4", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 5: groupKey=unknown should use default group (partitions: [4,5,6,7])
    partition = partitioner.partition("test-topic", "unknown", "span-4", 8);
    assertTrue(partition >= 4 && partition <= 7);
  }

  @Test
  public void testPartitionerWithKeyHashDelegatePartitioner() {
    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", configServiceClient, groupKeyExtractor, keyHashPartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0,1])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 8);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 4: groupKey=unknown should use default group [4,5,6,7]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 8);
    assertTrue(partition >= 4 && partition <= 7);

    // Test case 5: groupKey=unknown should use default group [4,5,6,7], key is null - should always
    // go to first partition in the group
    partition = partitioner.partition("test-topic", null, "span-5", 8);
    assertEquals(4, partition);
    partition = partitioner.partition("test-topic", null, "span-6", 8);
    assertEquals(4, partition);
    partition = partitioner.partition("test-topic", null, "span-7", 8);
    assertEquals(4, partition);
    partition = partitioner.partition("test-topic", null, "span-8", 8);
    assertEquals(4, partition);
  }

  @Test
  public void testWithNonMultipleWeightRatio() {

    int testCount = 100;
    PartitionerConfigServiceClient testClient =
        (profileName) ->
            new WeightedGroupProfile(
                PartitionerProfile.newBuilder()
                    .addGroups(newPartitionerGroup("group1", new String[] {"tenant-1"}, 27))
                    .addGroups(
                        newPartitionerGroup("group2", new String[] {"tenant-2", "tenant-3"}, 27))
                    .setDefaultGroupWeight(52)
                    .setName(profileName)
                    .build());

    WeightedGroupPartitioner<String, String> partitioner =
        new WeightedGroupPartitioner<>(
            "spans", testClient, groupKeyExtractor, roundRobinPartitioner);
    int partition;

    // Test case 1: tenant-1 belong to group-1 (partitions: [0 to 7])
    for (int i = 1; i <= testCount; i++) {
      partition = partitioner.partition("test-topic", "tenant-1", "span-" + i, 32);
      assertTrue(
          partition >= 0 && partition <= 7,
          "actual partition not in expected range. partition: " + partition);
    }

    // Test case 2: tenant-2 belong to group-2 (partitions: [8 to 15])
    for (int i = 1; i <= testCount; i++) {
      partition = partitioner.partition("test-topic", "tenant-2", "span-" + i, 32);
      assertTrue(
          partition >= 8 && partition <= 15,
          "actual partition not in expected range. partition: " + partition);
    }

    // Test case 3: tenant-3 belong to group-2 (partitions: [8 to 15])
    for (int i = 1; i <= testCount; i++) {
      partition = partitioner.partition("test-topic", "tenant-3", "span-" + i, 32);
      assertTrue(
          partition >= 8 && partition <= 15,
          "actual partition not in expected range. partition: " + partition);
    }

    // Test case 4: groupKey=unknown should use default group [16 to 31]
    for (int i = 1; i <= testCount; i++) {
      partition = partitioner.partition("test-topic", "unknown", "span-" + i, 32);
      assertTrue(
          partition >= 16 && partition <= 31,
          "actual partition not in expected range. partition: " + partition);
    }
  }

  private PartitionerConfigServiceClient getTestServiceClient() {
    return (profileName) ->
        new WeightedGroupProfile(
            PartitionerProfile.newBuilder()
                .addGroups(newPartitionerGroup("group1", new String[] {"tenant-1"}, 25))
                .addGroups(newPartitionerGroup("group2", new String[] {"tenant-2", "tenant-3"}, 25))
                .setDefaultGroupWeight(50)
                .setName(profileName)
                .build());
  }

  private PartitionerGroup newPartitionerGroup(String groupName, String[] members, int weight) {
    return PartitionerGroup.newBuilder()
        .setName(groupName)
        .addAllMemberIds(Arrays.asList(members))
        .setWeight(weight)
        .build();
  }
}
