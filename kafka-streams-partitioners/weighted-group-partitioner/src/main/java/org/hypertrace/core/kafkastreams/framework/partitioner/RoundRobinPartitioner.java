package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RoundRobinPartitioner<K, V> implements StreamPartitioner<K, V> {
  private final AtomicInteger currentPartition = new AtomicInteger();

  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    return Utils.toPositive(currentPartition.getAndIncrement()) % numPartitions;
  }
}
