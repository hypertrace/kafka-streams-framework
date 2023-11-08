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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
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
public class KafkaEventConsumer<K, V, D, C> extends Thread {
  private static final String EVENT_CONSUMER_ERROR_COUNT = "event.consumer.error.count";
  private static final String TOPIC_NAME = "topic.name";
  private static final String POLL_TIMEOUT = "poll.timeout";
  private final List<TopicPartition> topicPartitions;
  private final KafkaConsumer<D, C> kafkaEventConsumer;
  private final Duration pollTimeout;
  private final Counter errorCounter;
  private final KafkaEventListener<K, V> kafkaEventListener;

  public KafkaEventConsumer(
      String consumerName,
      Config kafkaConfig,
      Deserializer<D> keyDeserializer,
      Deserializer<C> valueDeserializer,
      KafkaEventListener<K, V> kafkaEventListener) {
    super(consumerName);
    this.setDaemon(true);
    this.kafkaEventListener = kafkaEventListener;
    Properties props =
        getKafkaConsumerConfigs(kafkaConfig.withFallback(getDefaultKafkaConsumerConfigs()));
    kafkaEventConsumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
    this.pollTimeout =
        kafkaConfig.hasPath(POLL_TIMEOUT)
            ? kafkaConfig.getDuration(POLL_TIMEOUT)
            : Duration.ofSeconds(30);
    String topic = kafkaConfig.getString(TOPIC_NAME);
    List<PartitionInfo> partitions = kafkaEventConsumer.partitionsFor(topic);
    topicPartitions =
        partitions.stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toList());
    kafkaEventConsumer.assign(topicPartitions);
    kafkaEventConsumer.seekToEnd(topicPartitions);
    this.errorCounter =
        PlatformMetricsRegistry.registerCounter(
            consumerName + "." + EVENT_CONSUMER_ERROR_COUNT, Collections.emptyMap());
  }

  @Override
  public void run() {
    do {
      try {
        ConsumerRecords<D, C> records = kafkaEventConsumer.poll(pollTimeout);
        records.forEach(r -> this.kafkaEventListener.actOnEvent(r.key(), r.value()));
        if (log.isDebugEnabled()) {
          for (TopicPartition partition : topicPartitions) {
            long position = kafkaEventConsumer.position(partition);
            log.debug(
                "Consumer state topic: {}, partition:{}, offset: {}",
                partition.topic(),
                partition.partition(),
                position);
          }
        }
      } catch (InterruptException interruptedException) {
        log.warn("Received interrupt exception from kafka poll ", interruptedException);
        kafkaEventConsumer.close();
        return;
      } catch (Exception ex) {
        this.errorCounter.increment();
        log.error("Consumer Error ", ex);
      }

    } while (true);
  }

  private Config getDefaultKafkaConsumerConfigs() {
    Map<String, String> defaultKafkaConsumerConfigMap = new HashMap<>();
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    defaultKafkaConsumerConfigMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return ConfigFactory.parseMap(defaultKafkaConsumerConfigMap);
  }

  private Properties getKafkaConsumerConfigs(Config configs) {
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
