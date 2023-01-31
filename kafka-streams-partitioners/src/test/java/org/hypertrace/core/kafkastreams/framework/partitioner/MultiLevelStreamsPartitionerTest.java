package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;
import org.junit.jupiter.api.Test;

public class MultiLevelStreamsPartitionerTest {
  private final ConfigProvider configProvider = getTestConfigProvider();
  private final BiFunction<String, String, String> groupKeyExtractor = (key, value) -> key;
  private final StreamPartitioner<String, String> delegatePartitioner =
      (topic, key, value, numPartitions) -> value.hashCode() % numPartitions;

  @Test
  public void testPartitionerWithNonOverlappingGroupPartitions() {
    MultiLevelStreamsPartitioner<String, String> partitioner =
        new MultiLevelStreamsPartitioner<>(configProvider, groupKeyExtractor, delegatePartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [4,5])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 8);
    assertTrue(partition >= 4 && partition <= 5);

    // Test case 2: tenant-2 belong to group-2 (partitions: [6,7])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 8);
    assertTrue(partition >= 6 && partition <= 7);

    // Test case 3: tenant-3 belong to group-2 (partitions: [6,7])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 8);
    assertTrue(partition >= 6 && partition <= 7);

    // Test case 4: key=4, groupKey=unknown should use default group [0,1,2,3]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 8);
    assertTrue(partition >= 0 && partition <= 3);
  }

  @Test
  public void testPartitionerWithOverlappingGroupPartitions() {
    MultiLevelStreamsPartitioner<String, String> partitioner =
        new MultiLevelStreamsPartitioner<>(configProvider, groupKeyExtractor, delegatePartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [1,2])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 3);
    assertTrue(partition >= 1 && partition <= 2);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 3);
    assertTrue(partition >= 2 && partition <= 2);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 3);
    assertTrue(partition >= 2 && partition <= 2);

    // Test case 4: groupKey=unknown should use default group [0,1]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 3);
    assertTrue(partition >= 0 && partition <= 1);
  }

  @Test
  public void testPartitionerWithSinglePartition() {
    MultiLevelStreamsPartitioner<String, String> partitioner =
        new MultiLevelStreamsPartitioner<>(configProvider, groupKeyExtractor, delegatePartitioner);

    // Test case 1: tenant-1 belong to group-1 (partitions: [1,2])
    int partition = partitioner.partition("test-topic", "tenant-1", "span-1", 1);
    assertTrue(partition == 0);

    // Test case 2: tenant-2 belong to group-2 (partitions: [2])
    partition = partitioner.partition("test-topic", "tenant-2", "span-2", 1);
    assertTrue(partition == 0);

    // Test case 3: tenant-3 belong to group-2 (partitions: [2])
    partition = partitioner.partition("test-topic", "tenant-3", "span-3", 1);
    assertTrue(partition == 0);

    // Test case 4: groupKey=unknown should use default group [0,1]
    partition = partitioner.partition("test-topic", "unknown", "span-4", 1);
    assertTrue(partition == 0);
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
