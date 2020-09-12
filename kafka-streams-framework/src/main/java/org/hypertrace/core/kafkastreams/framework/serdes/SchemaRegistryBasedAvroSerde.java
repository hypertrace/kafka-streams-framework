package org.hypertrace.core.kafkastreams.framework.serdes;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class SchemaRegistryBasedAvroSerde<T extends SpecificRecord> implements Deserializer<T>,
    Serializer<T> {

  private final Class<T> clazz;
  private transient KafkaAvroSerializer serializer;
  private transient KafkaAvroDeserializer deserializer;

  public SchemaRegistryBasedAvroSerde(Class<T> clazz) {
    this.clazz = clazz;
    this.serializer = new KafkaAvroSerializer();
    this.deserializer = new KafkaAvroDeserializer();
  }

  public void configure(Map<String, ?> serdeConfig, boolean isKey) {
    this.serializer.configure(serdeConfig, isKey);
    this.deserializer.configure(serdeConfig, isKey);
  }

  public byte[] serialize(String topic, T data) {
    return this.serializer.serialize(topic, data);
  }

  public T deserialize(String topic, byte[] data) {
    return (T) this.deserializer.deserialize(topic, data);
  }

  public void close() {
  }
}
