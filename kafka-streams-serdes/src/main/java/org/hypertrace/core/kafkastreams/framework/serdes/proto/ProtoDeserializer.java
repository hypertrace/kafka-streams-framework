package org.hypertrace.core.kafkastreams.framework.serdes.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoDeserializer<T extends Message> implements Deserializer<T> {

  private final Parser<T> parser;

  public ProtoDeserializer(Parser<T> parser) {
    this.parser = parser;
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    try {
      return parser.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
