package org.hypertrace.core.kafkastreams.framework.partitioner;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class ValueHashPartitioner<K, V> implements StreamPartitioner<K, V> {
  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    return Utils.toPositive(value.hashCode()) % numPartitions;
  }
}
