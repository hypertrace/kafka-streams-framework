package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.BiFunction;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;
import org.junit.jupiter.api.Test;

public class MultiLevelStreamPartitionerTest {
  private final ConfigProvider configProvider = getTestConfigProvider();
  private final BiFunction<String, String, String> groupKeyExtractor = (key, value) -> key;

  @Test
  public void testPartitionerWithNonOverlappingGroupPartitions() {
    MultiLevelStreamPartitioner<String, String> partitioner =
        new MultiLevelStreamPartitioner<>(configProvider, groupKeyExtractor, null);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0,])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 8);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2,3])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 8);
    assertTrue(partition >= 2 && partition <= 3);

    // Test case 4: key=4, groupKey=unknown should use default group [4,5,6,7]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 8);
    assertTrue(partition >= 4 && partition <= 7);
  }

  @Test
  public void testPartitionerWithOverlappingGroupPartitions() {
    MultiLevelStreamPartitioner<String, String> partitioner =
        new MultiLevelStreamPartitioner<>(configProvider, groupKeyExtractor, null);

    // Test case 1: tenant-1 belong to group-1 (partitions: [0])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 3);
    assertEquals(0, partition);

    // Test case 2: tenant-2 belong to group-2 (partitions: [0,1])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 3);
    assertTrue(partition >= 1 && partition <= 2);

    // Test case 3: tenant-3 belong to group-2 (partitions: [0,1])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 3);
    assertTrue(partition >= 0 && partition <= 1);

    // Test case 4: groupKey=unknown should use default group [1,2]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 3);
    assertTrue(partition >= 1 && partition <= 2);
  }

  @Test
  public void testPartitionerWithSinglePartition() {
    MultiLevelStreamPartitioner<String, String> partitioner =
        new MultiLevelStreamPartitioner<>(configProvider, groupKeyExtractor, null);

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

  private ConfigProvider getTestConfigProvider() {
    return () ->
        new MultiLevelPartitionerConfig(
            PartitionerProfile.newBuilder()
                .addGroups(newPartitionerGroup("group1", new String[] {"tenant-1"}, 25))
                .addGroups(newPartitionerGroup("group2", new String[] {"tenant-2", "tenant-3"}, 25))
                .setDefaultGroupWeight(50)
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
