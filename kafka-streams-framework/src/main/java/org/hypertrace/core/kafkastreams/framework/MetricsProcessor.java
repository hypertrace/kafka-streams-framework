package org.hypertrace.core.kafkastreams.framework;

import java.time.Instant;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public class MetricsProcessor implements Processor {

  @Override
  public void process(Record record) {
    System.out.println("Hello world");
    System.out.println(record.timestamp());
    Instant timestamp = Instant.ofEpochMilli(record.timestamp());
    System.out.println(timestamp); // e.g., 2026-03-10T12:17:00Z
  }
}
