package org.hypertrace.core.kafkastreams.framework.serdes;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerde<T extends SpecificRecordBase> implements Serde<T> {

  @Override
  public Serializer<T> serializer() {
    return new AvroSerializer<>();
  }

  @Override
  public Deserializer<T> deserializer() {
    return new AvroDeserializer<>();
  }
}
