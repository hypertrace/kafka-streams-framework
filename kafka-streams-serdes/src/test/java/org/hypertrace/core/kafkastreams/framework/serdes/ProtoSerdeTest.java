package org.hypertrace.core.kafkastreams.framework.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.hypertrace.core.kafkastreams.framework.serdes.proto.ProtoDeserializer;
import org.hypertrace.core.kafkastreams.framework.serdes.proto.ProtoSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import proto.TestProtoRecordOuterClass.TestProtoRecord;

public class ProtoSerdeTest {

  private static final String TEST_TOPIC = "test-topic";

  // Subclass for testing with proto deserialization
  public static class TestProtoRecordDeserializer extends ProtoDeserializer<TestProtoRecord> {
    public TestProtoRecordDeserializer() {
      super(TestProtoRecord.parser());
    }
  }

  @Test
  public void testSerialize() {
    Serializer<TestProtoRecord> serializer = new ProtoSerializer<>();

    Deserializer<TestProtoRecord> deserializer = new TestProtoRecordDeserializer();
    TestProtoRecord message = TestProtoRecord.newBuilder().setId("id").build();

    byte[] serializedData = serializer.serialize(TEST_TOPIC, message);

    Assertions.assertNotNull(serializedData);
    Assertions.assertTrue(serializedData.length > 0);

    TestProtoRecord deserializedMessage = deserializer.deserialize(TEST_TOPIC, serializedData);

    Assertions.assertEquals(message.getId(), deserializedMessage.getId());
  }
}
