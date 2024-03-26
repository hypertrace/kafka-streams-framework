package org.hypertrace.core.kafka.event.listener;

import static org.hypertrace.core.kafka.event.listener.KafkaConsumerUtils.POLL_TIMEOUT;
import static org.hypertrace.core.kafka.event.listener.KafkaConsumerUtils.TOPIC_NAME;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
class KafkaLiveEventListenerCallable<K, V> implements Callable<Void> {
  private static final String EVENT_CONSUMER_ERROR_COUNT = "event.consumer.error.count";
  private final List<TopicPartition> topicPartitions;
  private final Consumer<K, V> kafkaConsumer;
  private final Duration pollTimeout;
  private final Counter errorCounter;
  private final Queue<BiConsumer<? super K, ? super V>> callbacks;

  KafkaLiveEventListenerCallable(
      String consumerName,
      Config kafkaConfig,
      Consumer<K, V> kafkaConsumer,
      Queue<BiConsumer<? super K, ? super V>> callbacks) {
    this.callbacks = callbacks;
    this.pollTimeout =
        kafkaConfig.hasPath(POLL_TIMEOUT)
            ? kafkaConfig.getDuration(POLL_TIMEOUT)
            : Duration.ofSeconds(30);
    String topic = kafkaConfig.getString(TOPIC_NAME);
    this.kafkaConsumer = kafkaConsumer;
    // fetch partitions and seek to end of partitions to consume live events
    List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);
    topicPartitions =
        partitions.stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toList());
    kafkaConsumer.assign(topicPartitions);
    kafkaConsumer.seekToEnd(topicPartitions);
    this.errorCounter =
        PlatformMetricsRegistry.registerCounter(
            consumerName + "." + EVENT_CONSUMER_ERROR_COUNT, Collections.emptyMap());
  }

  @Override
  public Void call() {
    do {
      try {
        ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeout);
        records.forEach(
            r -> this.callbacks.forEach(callback -> callback.accept(r.key(), r.value())));
        if (log.isDebugEnabled()) {
          for (TopicPartition partition : topicPartitions) {
            long position = kafkaConsumer.position(partition);
            log.debug(
                "Consumer state topic: {}, partition:{}, offset: {}",
                partition.topic(),
                partition.partition(),
                position);
          }
        }
      } catch (InterruptException interruptedException) {
        log.warn("Received interrupt exception from kafka poll ", interruptedException);
        kafkaConsumer.close();
        return null;
      } catch (Exception ex) {
        this.errorCounter.increment();
        log.error("Consumer Error ", ex);
      }

    } while (true);
  }
}
