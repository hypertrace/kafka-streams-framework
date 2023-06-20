package org.hypertrace.core.kafkastreams.framework;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.hypertrace.core.kafkastreams.framework.rocksdb.BoundedMemoryConfigSetter;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@SetEnvironmentVariable(key = "SERVICE_NAME", value = "sample-kafka-streams-service")
public class SampleAsyncAppTest {

  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private SampleAsyncApp sampleApp;
  private Properties streamsConfig;

  @BeforeEach
  public void setup() {
    sampleApp = new SampleAsyncApp(ConfigClientFactory.getClient());

    sampleApp.doInit();
    streamsConfig = new Properties();
    streamsConfig.putAll(sampleApp.streamsConfig);

    td = new TopologyTestDriver(sampleApp.topology, streamsConfig);

    inputTopic =
            td.createInputTopic(
                    SampleApp.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic =
            td.createOutputTopic(
                    SampleApp.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @SneakyThrows
  @Test
  public void asyncTransformationTest() {
    assertThat(outputTopic.isEmpty(), is(true));

    int batchSize = 1000;
    for (int i = 1; i <= batchSize; i++) {
      inputTopic.pipeInput("key-" + i, "value-" + i);
    }

    // sleep time has to be bigger than the commit interval configured for the test
    // otw, test will be flaky
    Thread.sleep(1000);
    inputTopic.pipeInput("key-final", "value-final");
    // test ordered processing. mandatory requirement.
    for (int i = 1; i <= batchSize; i++) {
      assertThat(outputTopic.readValue(), endsWith("value-" + i));
    }

    // read the final record
    outputTopic.readKeyValue();

    assertThat(outputTopic.isEmpty(), is(true));
  }

  @SneakyThrows
  @Test
  public void testUnderlyingReturnsNull() {
    assertThat(outputTopic.isEmpty(), is(true));
    // null should be handled without erring out.
    inputTopic.pipeInput("discarded-key", "discarded-value");

    Thread.sleep(1000);
    inputTopic.pipeInput("key-final", "value-final");

    // read the final record
    outputTopic.readKeyValue();
    assertThat(outputTopic.isEmpty(), is(true));
  }

  @Test
  public void baseStreamsConfigTest() {
    Map<String, Object> baseStreamsConfig = sampleApp.getBaseStreamsConfig();
    assertThat(
        baseStreamsConfig.get(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG),
        is(BoundedMemoryConfigSetter.class));
    assertThat(baseStreamsConfig.get(DEFAULT_KEY_SERDE_CLASS_CONFIG), is(SpecificAvroSerde.class));
    assertThat(
        baseStreamsConfig.get(DEFAULT_VALUE_SERDE_CLASS_CONFIG), is(SpecificAvroSerde.class));
    assertThat(
        baseStreamsConfig.get(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG),
        is(LogAndContinueExceptionHandler.class));
    assertThat(baseStreamsConfig.get(producerPrefix(ACKS_CONFIG)), is("all"));
  }
}
