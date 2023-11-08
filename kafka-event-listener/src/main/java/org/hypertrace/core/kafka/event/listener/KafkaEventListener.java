package org.hypertrace.core.kafka.event.listener;

import java.time.Duration;
import java.util.List;

public abstract class KafkaEventListener<K, V> {
  private final List<KafkaEventConsumer<K, V, Object, Object>> eventConsumers;

  public KafkaEventListener(List<KafkaEventConsumer<K, V, Object, Object>> eventConsumers) {
    this.eventConsumers = eventConsumers;
    eventConsumers.forEach(Thread::start);
  }

  public abstract <D, C> void actOnEvent(D eventKey, C eventValue);

  public void close() {
    eventConsumers.forEach(Thread::interrupt);
    eventConsumers.forEach(
        consumer -> {
          try {
            consumer.join(Duration.ofSeconds(10).toMillis());
          } catch (InterruptedException e) {
          }
        });
  }
}
