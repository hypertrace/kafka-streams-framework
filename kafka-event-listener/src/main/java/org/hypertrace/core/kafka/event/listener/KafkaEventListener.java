package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class KafkaEventListener<K, V> {
  private final List<KafkaEventListenerConsumer<K, V, Object, Object>> eventListenerConsumers =
      new ArrayList<>();

  public <D, C> void registerConsumer(
      String consumerName,
      Config kafkaConfig,
      Deserializer<D> keyDeserializer,
      Deserializer<C> valueDeserializer) {
    registerConsumer(
        new KafkaEventListenerConsumer<>(
            consumerName, kafkaConfig, keyDeserializer, valueDeserializer, this));
  }

  /** only for test usage */
  <D, C> void registerConsumer(KafkaEventListenerConsumer<K, V, D, C> eventListenerConsumer) {
    eventListenerConsumer.start();
    eventListenerConsumers.add(
        (KafkaEventListenerConsumer<K, V, Object, Object>) eventListenerConsumer);
  }

  /** this method needs to be concurrent safe if registering multiple consumers */
  public abstract <D, C> void actOnEvent(D eventKey, C eventValue);

  public void close() {
    eventListenerConsumers.forEach(Thread::interrupt);
    eventListenerConsumers.forEach(
        consumer -> {
          try {
            consumer.join(Duration.ofSeconds(10).toMillis());
          } catch (InterruptedException e) {
          }
        });
  }
}
