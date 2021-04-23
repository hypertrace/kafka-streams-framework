package org.hypertrace.core.kafkastreams.framework;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class SampleAppTest {

  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private SampleApp sampleApp;
  private Properties streamsConfig;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "sample-kafka-streams-service")
  public void setup() {
    Config config = ConfigFactory.parseURL(
        getClass().getClassLoader()
            .getResource("configs/sample-kafka-streams-service/application.conf"));
    sampleApp = new SampleApp(getConfigClientMock(config));

    sampleApp.doInit();
    streamsConfig = new Properties();
    streamsConfig.putAll(sampleApp.streamsConfig);
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @Test
  public void shouldIncludeValueWithLengthGreaterThanFive() {
    td = new TopologyTestDriver(sampleApp.topology, streamsConfig);

    inputTopic = td.createInputTopic(SampleApp.INPUT_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer());
    outputTopic = td.createOutputTopic(SampleApp.OUTPUT_TOPIC, Serdes.String().deserializer(),
        Serdes.String().deserializer());

    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "barrrrr");
    assertThat(outputTopic.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "bar");
    assertThat(outputTopic.isEmpty(), is(true));
  }

  private ConfigClient getConfigClientMock(Config config) {
    return new ConfigClient() {
      @Override
      public Config getConfig() {
        return config;
      }

      @Override
      public Config getConfig(String s, String s1, String s2, String s3) {
        return config;
      }
    };
  }
}
