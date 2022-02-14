package org.hypertrace.core.kafkastreams.framework.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * The partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>Otherwise choose the sticky non-excluded partition that changes when the batch is full.
 *
 *
 * NOTE: In contrast to the DefaultPartitioner, the record key is NOT used as part of the partitioning strategy in this
 *       partitioner. Records with the same key are not guaranteed to be sent to the same partition.
 *
 * NOTE: Exclusion of partitions is done best-effort basis. There are some scenarios, where this
 *       partitioner. Records with the same key are not guaranteed to be sent to the same partition.
 * See KIP-480 for details about sticky partitioning.
 */
public class FilteredUniformStickyPartitioner implements Partitioner {

  private final FilteredUniformStickyPartitionCache stickyPartitionCache =
      new FilteredUniformStickyPartitionCache();

  @Override
  public void configure(Map<String, ?> configs) {
    stickyPartitionCache.configure(configs);
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return stickyPartitionCache.partition(topic, cluster);
  }

  @Override
  public void close() {
  }

  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
  }
}
