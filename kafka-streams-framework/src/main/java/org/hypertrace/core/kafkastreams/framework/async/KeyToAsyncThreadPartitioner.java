package org.hypertrace.core.kafkastreams.framework.async;

public interface KeyToAsyncThreadPartitioner<K> {
  int getNumericalHashForKey(K key);
}
