package org.hypertrace.core.kafkastreams.framework.interceptors;

import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class CachingInterceptor implements Processor<String, String, Void, Void> {

  private List<KeyValue<String, String>> pairs;

  public CachingInterceptor(List<KeyValue<String, String>> pairs) {
    this.pairs = pairs;
  }

  @Override
  public void process(Record<String, String> record) {
    pairs.add(new KeyValue<>(record.key(), record.value()));
  }
}
