package org.hypertrace.core.kafkastreams.framework;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class AsyncProcessor implements Processor {
  @Override
  public void process(Record record) {}
}
