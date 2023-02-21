package org.hypertrace.core.kafkastreams.framework.partitioner;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class KeyHashPartitioner<K, V> implements StreamPartitioner<K, V> {
  @Override
  public Integer partition(String topic, K key, V value, int numPartitions) {
    int hashcode = 0;
    if (key != null) {
      hashcode = key.hashCode();
    }
    return Utils.toPositive(hashcode) % numPartitions;
  }
}
