package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import io.micrometer.core.instrument.Counter;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class MetricsInterceptor implements Processor {

  private final Counter timeLagCounter;
  private final Counter numRecordsCounter;

  MetricsInterceptor(Counter numRecordsCounter, Counter timeLagCounter) {
    this.numRecordsCounter = numRecordsCounter;
    this.timeLagCounter = timeLagCounter;
  }

  @Override
  public void process(Record record) {
    timeLagCounter.increment(System.currentTimeMillis() - record.timestamp());
    numRecordsCounter.increment();
  }
}
