package org.hypertrace.core.kafkastreams.framework;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@SetEnvironmentVariable(key = "SERVICE_NAME", value = "sample-consolidated-kafka-streams-service")
public class ConsolidatedServiceTest {
  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic1;
  private TestOutputTopic<String, String> outputTopic1;

  private ConsolidatedService underTest;
  private Properties streamsConfig;

  private TestInputTopic<String, String> inputTopic2;
  private TestOutputTopic<String, String> outputTopic2;


  @BeforeEach
  public void setup() {
    underTest = new ConsolidatedService(ConfigClientFactory.getClient());

    underTest.doInit();
    streamsConfig = new Properties();
    streamsConfig.putAll(underTest.streamsConfig);

    td = new TopologyTestDriver(underTest.topology, streamsConfig);
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @Test
  public void checkDataFlowsThroughBothSubTopologies() {
    inputTopic1 = td.createInputTopic(Service1.INPUT_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer());
    outputTopic1 = td.createOutputTopic(Service1.OUTPUT_TOPIC, Serdes.String().deserializer(),
        Serdes.String().deserializer());

    inputTopic2 = td.createInputTopic(Service2.INPUT_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer());
    outputTopic2 = td.createOutputTopic(Service2.OUTPUT_TOPIC, Serdes.String().deserializer(),
        Serdes.String().deserializer());

    assertThat(outputTopic1.isEmpty(), is(true));
    assertThat(outputTopic2.isEmpty(), is(true));

    inputTopic1.pipeInput("foo", "barrrrr");
    inputTopic2.pipeInput("foo", "barrrrr");
    assertThat(outputTopic1.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic1.isEmpty(), is(true));
    assertThat(outputTopic2.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic2.isEmpty(), is(true));

    inputTopic1.pipeInput("foo", "bar");
    inputTopic2.pipeInput("foo", "bar");
    assertThat(outputTopic1.isEmpty(), is(true));
    assertThat(outputTopic2.isEmpty(), is(true));
  }

}
