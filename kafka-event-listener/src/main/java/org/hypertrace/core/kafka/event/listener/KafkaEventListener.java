package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * KafkaEventListener consumes events from a single Kafka Topic and on every message invokes
 * provided callbacks. The callback invocation is done in a separate thread and needs to be
 * concurrent safe.
 */
public class KafkaEventListener<K, V> implements AutoCloseable {
  private final KafkaEventListenerThread<K, V> kafkaEventListenerThread;
  private final ExecutorService executorService;
  private final boolean cleanupExecutor;

  private KafkaEventListener(
      KafkaEventListenerThread<K, V> kafkaEventListenerThread,
      ExecutorService executorService,
      boolean cleanupExecutor) {
    this.kafkaEventListenerThread = kafkaEventListenerThread;
    this.executorService = executorService;
    this.cleanupExecutor = cleanupExecutor;
    executorService.submit(kafkaEventListenerThread);
  }

  @Override
  public void close() throws Exception {
    kafkaEventListenerThread.interrupt();
    kafkaEventListenerThread.join(Duration.ofSeconds(10).toMillis());
    if (cleanupExecutor) {
      executorService.shutdown();
    }
  }

  public static final class Builder<K, V> {
    List<BiConsumer<? super K, ? super V>> callbacks = new ArrayList<>();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    boolean cleanupExecutor =
        true; // if builder creates executor shutdown executor while closing event listener

    public Builder<K, V> registerCallback(BiConsumer<? super K, ? super V> callbackFunction) {
      callbacks.add(callbackFunction);
      return this;
    }

    public Builder<K, V> withExecutorService(ExecutorService executorService) {
      this.executorService = executorService;
      this.cleanupExecutor = false;
      return this;
    }

    public KafkaEventListener<K, V> build(
        String consumerName, Config kafkaConfig, Consumer<K, V> kafkaConsumer) {
      assertCallbacksPresent();
      return new KafkaEventListener<>(
          new KafkaEventListenerThread<>(consumerName, kafkaConfig, kafkaConsumer, callbacks),
          executorService,
          cleanupExecutor);
    }

    public KafkaEventListener<K, V> build(
        String consumerName,
        Config kafkaConfig,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer) {
      assertCallbacksPresent();
      return new KafkaEventListener<>(
          new KafkaEventListenerThread<>(
              consumerName, kafkaConfig, keyDeserializer, valueDeserializer, callbacks),
          executorService,
          cleanupExecutor);
    }

    private void assertCallbacksPresent() {
      if (callbacks.isEmpty()) {
        throw new IllegalArgumentException("no call backs are provided to KafkaEventListener");
      }
    }
  }
}
