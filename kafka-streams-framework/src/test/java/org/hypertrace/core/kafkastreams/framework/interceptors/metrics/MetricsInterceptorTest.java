package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsInterceptorTest {

  @BeforeEach
  void setup() {
    Map<String, Object> config = new HashMap<>();
    config.put("reporter.names", List.of("testing"));

    PlatformMetricsRegistry.initMetricsRegistry("test", ConfigFactory.parseMap(config));
  }

  @Test
  public void testOnConsume() {
    TopicPartition tp = new TopicPartition("test-topic", 0);
    List<ConsumerRecord<Object, Object>> records =
        List.of(
            new ConsumerRecord<>("test-topic", 0, 0, "k1", "v1"),
            new ConsumerRecord<>("test-topic", 0, 1, "k2", null));

    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = Map.of(tp, records);
    ConsumerRecords<Object, Object> input = new ConsumerRecords<>(map);
    MetricsInterceptor interceptor = new MetricsInterceptor();
    interceptor.configure(Maps.newTreeMap());
    ConsumerRecords<Object, Object> output = interceptor.onConsume(input);
    assertEquals(2, output.count());

    assertEquals(
        2.0, PlatformMetricsRegistry.getMeterRegistry().counter("kafka_records_count").count());
    assertNotEquals(
        0, PlatformMetricsRegistry.getMeterRegistry().counter("kafka_records_time_lag").count());
  }
}
