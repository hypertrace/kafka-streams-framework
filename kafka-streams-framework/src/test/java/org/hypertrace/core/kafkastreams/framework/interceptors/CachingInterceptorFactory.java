package org.hypertrace.core.kafkastreams.framework.interceptors;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;

public class CachingInterceptorFactory {
  private List<KeyValue<String, String>> pairs;

  CachingInterceptorFactory() {
    this.pairs = new ArrayList<>();
  }

  CachingInterceptor create() {
    return new CachingInterceptor(pairs);
  }

  List<KeyValue<String, String>> getCache() {
    return pairs;
  }
}
