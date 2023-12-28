package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaConsumerUtils {
  public static final String TOPIC_NAME = "topic.name"; // required key in kafkaConfig
  public static final String POLL_TIMEOUT = "poll.timeout"; // defaults to 30s if not provided

  /**
   * Returns a kafka consumer for provided config and key value deserializers. Only one instance of
   * consumer should be required per pod, ensure singleton.
   */
  public static <K, V> Consumer<K, V> getKafkaConsumer(
      Config kafkaConfig, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    return new KafkaConsumer<>(
        getKafkaConsumerConfigs(kafkaConfig.withFallback(getDefaultKafkaConsumerConfigs())),
        keyDeserializer,
        valueDeserializer);
  }

  private static Properties getKafkaConsumerConfigs(Config configs) {
    Map<String, String> configMap = new HashMap<>();
    Set<Map.Entry<String, ConfigValue>> entries = configs.entrySet();
    for (Map.Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      configMap.put(key, configs.getString(key));
    }
    Properties props = new Properties();
    props.putAll(configMap);
    return props;
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
}
