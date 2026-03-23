package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import io.micrometer.core.instrument.Counter;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class MetricsInterceptor implements ConsumerInterceptor<Object, Object> {

  private final Counter timeLagCounter;
  private final Counter numRecordsCounter;

  MetricsInterceptor(Counter numRecordsCounter, Counter timeLagCounter) {
    this.numRecordsCounter = numRecordsCounter;
    this.timeLagCounter = timeLagCounter;
  }

  @Override
  public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
    for (ConsumerRecord<Object, Object> record : records) {
      timeLagCounter.increment(System.currentTimeMillis() - record.timestamp());
      numRecordsCounter.increment();
    }
    return records;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public void onCommit(Map offsets) {
    // no-op
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // no-op
  }
}
