package org.hypertrace.core.kafkastreams.framework.interceptors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StreamsBuilderWithInterceptorTest {

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private StreamsBuilder streamsBuilder;
  private Topology topology;
  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private Properties streamsConfig;

  CachingInterceptorFactory factory;

  @BeforeEach
  public void setup() {

    factory = new CachingInterceptorFactory();
    streamsBuilder = new StreamsBuilderWithInterceptor(List.of(factory::create));
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.to(OUTPUT_TOPIC);
    topology = streamsBuilder.build();

    streamsConfig = new Properties();
    streamsConfig.put("application.id", "test-interceptor");
    streamsConfig.put("bootstrap.servers", "dummy:1234");
    streamsConfig.put("default.key.serde", Serdes.StringSerde.class.getName());
    streamsConfig.put("default.value.serde", Serdes.StringSerde.class.getName());
    td = new TopologyTestDriver(topology, streamsConfig);
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @Test
  public void interceptorTest() {
    inputTopic =
        td.createInputTopic(
            INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic =
        td.createOutputTopic(
            OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("fooooo", "barrrrr");
    assertThat(outputTopic.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "bar");
    assertThat(outputTopic.readValue(), equalTo("bar"));
    assertThat(outputTopic.isEmpty(), is(true));

    List<KeyValue<String, String>> cache = factory.getCache();
    assertThat(cache.size(), is(2));
    KeyValue<String, String> pair = cache.get(0);
    assertEquals("fooooo", pair.key);
    assertEquals("barrrrr", pair.value);

    pair = cache.get(1);
    assertEquals("foo", pair.key);
    assertEquals("bar", pair.value);
  }
}
