package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import io.micrometer.core.instrument.Counter;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class MetricsInterceptor implements ConsumerInterceptor<Object, Object> {
  private static final String TIME_LAG_COUNTER_NAME = "kafka_records_time_lag";
  private static final String NUM_RECORDS_COUNTER_NAME = "kafka_records_count";
  private Counter timeLagCounter;
  private Counter numRecordsCounter;

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
    this.timeLagCounter = PlatformMetricsRegistry.getMeterRegistry().counter(TIME_LAG_COUNTER_NAME);
    this.numRecordsCounter =
        PlatformMetricsRegistry.getMeterRegistry().counter(NUM_RECORDS_COUNTER_NAME);
  }
}
