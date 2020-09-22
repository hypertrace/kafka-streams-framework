package org.hypertrace.core.kafkastreams.framework.serdes;

import io.confluent.common.utils.Utils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroSerdeTest {

  @Test
  public void testSerde(){
    AvroSerde serde = new AvroSerde();
    Headers headers = new RecordHeaders();
    headers.add("sample-header-key", Utils.utf8("sample-header-value"));
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();
    final byte[] bytes = serde.serializer().serialize("topic-name", headers, serializedRecord);
    final TestRecord desrializedRecord = (TestRecord) serde.deserializer().deserialize("topic-name", headers, bytes);
    Assertions.assertEquals(serializedRecord, desrializedRecord, "Serialized record is not matching with deserialized");
    Assertions.assertEquals(serializedRecord.getId(), desrializedRecord.getId());
    Assertions.assertEquals(serializedRecord.getName(), desrializedRecord.getName());
  }
}
