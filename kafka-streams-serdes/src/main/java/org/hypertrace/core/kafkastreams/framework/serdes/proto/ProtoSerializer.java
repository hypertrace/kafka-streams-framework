package org.hypertrace.core.kafkastreams.framework.serdes.proto;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Custom Proto Serializer for Kafka.
 *
 * <p>This class provides a serialization mechanism for Kafka messages using Protocol Buffers
 * without schema validation. It extends the Kafka Serializer interface and allows for direct
 * serialization of byte arrays into Proto message objects by utilizing the provided Parser for the
 * specific Proto message type.
 *
 * <p>Motivation: Since the proto. configurations are usually shared between the producer and the
 * consumers,the field descriptors are well-known to both the parties. In cases when there are other
 * mechanisms to validate proto. compatibilities schema validation becomes redundant and this class
 * can be used in such cases. The built-in {@code kafkaProtoSerdes} from Confluent performs schema
 * validation via the schema registry service, which introduces overhead. This custom serializer
 * eliminates that overhead, simplifying the processing flow by bypassing schema validation.
 *
 * <p>Usage: To use this class, create a subclass specifying the Proto message type, and configure
 * Kafka to use the custom serializer.
 *
 * <p>Example:
 *
 * <pre>{@code
 * public class MyProtoMessageSerializer extends ProtoSerializer<MyProtoMessage> {
 *
 * }
 * }</pre>
 *
 * Then, configure Kafka to use this serializer:
 *
 * <pre>{@code
 * key.serializer=com.example.MyProtoMessageSerializer
 * }</pre>
 *
 * @param <T> The Proto message type to be serialized.
 */
public class ProtoSerializer<T extends Message> implements Serializer<T> {
  @Override
  public byte[] serialize(String topic, T data) {
    return data.toByteArray();
  }
}
