package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import lombok.Value;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * KafkaEventListener consumes events from a single Kafka Topic and on every message invokes
 * provided callbacks. The callback invocation is done in a separate thread and needs to be
 * concurrent safe.
 */
public class KafkaEventListener<K, V> implements AutoCloseable {
  private final KafkaEventListenerThread<K, V> kafkaEventListenerThread;

  private KafkaEventListener(KafkaEventListenerThread<K, V> kafkaEventListenerThread) {
    this.kafkaEventListenerThread = kafkaEventListenerThread;
  }

  @Override
  public void close() throws Exception {
    kafkaEventListenerThread.interrupt();
    kafkaEventListenerThread.join(Duration.ofSeconds(10).toMillis());
  }

  public static final class Builder<K, V> {
    KafkaEventListenerThreadConfig<K, V> kafkaEventListenerThreadConfig;
    List<BiConsumer<? super K, ? super V>> callbacks = new ArrayList<>();
    ExecutorService executorService;

    public Builder<K, V> addKafkaTopic(
        String consumerName,
        Config kafkaConfig,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer) {
      assertAbsenceOfKakfaTopic();
      kafkaEventListenerThreadConfig =
          new KafkaEventListenerThreadConfig<>(
              consumerName, kafkaConfig, keyDeserializer, valueDeserializer, null, false);
      return this;
    }

    public Builder<K, V> addKafkaConsumer(
        String consumerName, Config kafkaConfig, Consumer<K, V> kafkaConsumer) {
      assertAbsenceOfKakfaTopic();
      kafkaEventListenerThreadConfig =
          new KafkaEventListenerThreadConfig<>(
              consumerName, kafkaConfig, null, null, kafkaConsumer, true);
      return this;
    }

    public Builder<K, V> registerCallback(BiConsumer<? super K, ? super V> callbackFunction) {
      callbacks.add(callbackFunction);
      return this;
    }

    public Builder<K, V> withExecutorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public KafkaEventListener<K, V> build() {
      if (Objects.isNull(kafkaEventListenerThreadConfig) || callbacks.isEmpty()) {
        throw new IllegalArgumentException(
            "one kafka topic and at least one callback are required for KafkaEventListener");
      }
      KafkaEventListenerThread<K, V> kafkaEventListenerThread =
          kafkaEventListenerThreadConfig.getThreadFromConfig(callbacks);
      if (Objects.isNull(executorService)) {
        executorService = Executors.newSingleThreadExecutor();
      }
      executorService.submit(kafkaEventListenerThread);
      return new KafkaEventListener<>(kafkaEventListenerThread);
    }

    private void assertAbsenceOfKakfaTopic() {
      if (Objects.nonNull(kafkaEventListenerThreadConfig)) {
        throw new IllegalArgumentException(
            "only one kafka topic is supported for KafkaEventListener, reuse callbacks across multiple instances of KafkaEventListeners");
      }
    }

    @Value
    private static class KafkaEventListenerThreadConfig<K, V> {
      String consumerName;
      Config kafkaConfig;
      Deserializer<K> keyDeserializer;
      Deserializer<V> valueDeserializer;
      Consumer<K, V> kafkaConsumer;
      boolean useKafkaConsumer;

      KafkaEventListenerThread<K, V> getThreadFromConfig(
          List<BiConsumer<? super K, ? super V>> callbacks) {
        if (useKafkaConsumer) {
          return new KafkaEventListenerThread<>(
              consumerName, kafkaConfig, kafkaConsumer, callbacks);
        }
        return new KafkaEventListenerThread<>(
            consumerName, kafkaConfig, keyDeserializer, valueDeserializer, callbacks);
      }
    }
  }
}
