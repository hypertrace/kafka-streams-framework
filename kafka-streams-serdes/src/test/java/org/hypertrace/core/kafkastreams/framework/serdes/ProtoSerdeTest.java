package org.hypertrace.core.kafkastreams.framework.serdes;

import com.google.protobuf.Value;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.hypertrace.core.kafkastreams.framework.serdes.proto.ProtoDeserializer;
import org.hypertrace.core.kafkastreams.framework.serdes.proto.ProtoSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtoSerdeTest {

  private static final String TEST_TOPIC = "test-topic";

  // Subclass for testing with proto deserialization
  public static class TestProtoRecordDeserializer extends ProtoDeserializer<Value> {
    public TestProtoRecordDeserializer() {
      super(Value.parser());
    }
  }

  @Test
  public void testSerialize() {
    Serializer<Value> serializer = new ProtoSerializer<>();

    Deserializer<Value> deserializer = new TestProtoRecordDeserializer();
    Value message = Value.newBuilder().setStringValue("id").build();

    byte[] serializedData = serializer.serialize(TEST_TOPIC, message);

    Assertions.assertNotNull(serializedData);
    Assertions.assertTrue(serializedData.length > 0);

    Value deserializedMessage = deserializer.deserialize(TEST_TOPIC, serializedData);

    Assertions.assertEquals(message.getStringValue(), deserializedMessage.getStringValue());
  }
}
