package org.hypertrace.core.kafkastreams.framework.serdes.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Custom Proto Deserializer for Kafka.
 *
 * <p>This class provides a deserialization mechanism for Kafka messages using Protocol Buffers
 * without schema validation. It extends the Kafka Deserializer interface and allows for direct
 * deserialization of byte arrays into Proto message objects by utilizing the provided Parser for
 * the specific Proto message type.
 *
 * <p>Motivation: In setups where both producers and consumers use the same Proto schemas, the need
 * for schema validation becomes redundant. The built-in {@code kafkaProtoSerdes} from Confluent
 * performs schema validation via the schema registry service, which introduces overhead. This
 * custom deserializer eliminates that overhead, simplifying the processing flow by bypassing schema
 * validation.
 *
 *
 * <p>Usage: To use this class, create a subclass specifying the Proto message type, pass the
 * corresponding Parser to the superclass constructor, and configure Kafka to use the custom
 * deserializer.
 *
 * <p>Example:
 *
 * <pre>{@code
 * public class MyProtoMessageDeserializer extends ProtoDeserializer<MyProtoMessage> {
 *     public MyProtoMessageDeserializer() {
 *         super(MyProtoMessage.parser());
 *     }
 * }
 * }</pre>
 *
 * Then, configure Kafka to use this deserializer:
 *
 * <pre>{@code
 * key.deserializer=com.example.MyProtoMessageDeserializer
 * }</pre>
 *
 * @param <T> The Proto message type to be deserialized.
 */
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
