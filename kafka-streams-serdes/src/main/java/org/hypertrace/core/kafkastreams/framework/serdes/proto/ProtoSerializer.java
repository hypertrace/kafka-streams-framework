package org.hypertrace.core.kafkastreams.framework.serdes.proto;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

public class ProtoSerializer<T extends Message> implements Serializer<T> {
  @Override
  public byte[] serialize(String topic, T data) {
    return data.toByteArray();
  }
}
