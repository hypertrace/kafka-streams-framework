package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class KafkaEventListenerListenerConsumer<D, C> extends Thread
    implements EventListenerConsumer {
  private static final String EVENT_CONSUMER_ERROR_COUNT = "event.consumer.error.count";
  private static final String TOPIC_NAME = "topic.name";
  private static final String POLL_TIMEOUT = "poll.timeout";
  private final List<TopicPartition> topicPartitions;
  private final Consumer<D, C> kafkaConsumer;
  private final Duration pollTimeout;
  private final Counter errorCounter;
  private final BiConsumer<D, C> listenerCallback;

  KafkaEventListenerListenerConsumer(
      String consumerName,
      Config kafkaConfig,
      Deserializer<D> keyDeserializer,
      Deserializer<C> valueDeserializer,
      BiConsumer<D, C> listenerCallback) {
    this(
        consumerName,
        kafkaConfig,
        new KafkaConsumer<>(
            getKafkaConsumerConfigs(kafkaConfig.withFallback(getDefaultKafkaConsumerConfigs())),
            keyDeserializer,
            valueDeserializer),
        listenerCallback);
  }

  KafkaEventListenerListenerConsumer(
      String consumerName,
      Config kafkaConfig,
      Consumer<D, C> kafkaConsumer,
      BiConsumer<D, C> listenerCallback) {
    super(consumerName);
    this.setDaemon(true);
    this.listenerCallback = listenerCallback;
    this.pollTimeout =
        kafkaConfig.hasPath(POLL_TIMEOUT)
            ? kafkaConfig.getDuration(POLL_TIMEOUT)
            : Duration.ofSeconds(30);
    String topic = kafkaConfig.getString(TOPIC_NAME);
    this.kafkaConsumer = kafkaConsumer;
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
  public void startConsumer() throws Exception {
    this.start();
  }

  @Override
  public void stopConsumer() throws Exception {
    this.interrupt();
    this.join(Duration.ofSeconds(10).toMillis());
  }

  @Override
  public void run() {
    do {
      try {
        ConsumerRecords<D, C> records = kafkaConsumer.poll(pollTimeout);
        records.forEach(r -> this.listenerCallback.accept(r.key(), r.value()));
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
        return;
      } catch (Exception ex) {
        this.errorCounter.increment();
        log.error("Consumer Error ", ex);
      }

    } while (true);
  }

  private static Config getDefaultKafkaConsumerConfigs() {
    Map<String, String> defaultKafkaConsumerConfigMap = new HashMap<>();
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return ConfigFactory.parseMap(defaultKafkaConsumerConfigMap);
  }

  private static Properties getKafkaConsumerConfigs(Config configs) {
    Map<String, String> configMap = new HashMap<>();
    Set<Entry<String, ConfigValue>> entries = configs.entrySet();
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      configMap.put(key, configs.getString(key));
    }
    Properties props = new Properties();
    props.putAll(configMap);
    return props;
  }
}
