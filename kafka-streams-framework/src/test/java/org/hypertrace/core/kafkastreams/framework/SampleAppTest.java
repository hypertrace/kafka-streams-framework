package org.hypertrace.core.kafkastreams.framework;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.rocksdb.BoundedMemoryConfigSetter;
import org.hypertrace.core.kafkastreams.framework.threading.StreamThreadsCountResolver;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@SetEnvironmentVariable(key = "SERVICE_NAME", value = "sample-kafka-streams-service")
public class SampleAppTest {

  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private SampleApp sampleApp;
  private Properties streamsConfig;

  @BeforeEach
  public void setup() {
    sampleApp = new SampleApp(ConfigClientFactory.getClient());

    sampleApp.doInit();
    streamsConfig = new Properties();
    streamsConfig.putAll(sampleApp.streamsConfig);

    td = new TopologyTestDriver(sampleApp.topology, streamsConfig);
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @Test
  public void shouldIncludeValueWithLengthGreaterThanFive() {
    inputTopic =
        td.createInputTopic(
            SampleApp.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic =
        td.createOutputTopic(
            SampleApp.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "barrrrr");
    assertThat(outputTopic.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "bar");
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

  // No resolver wired up → framework must keep the configured num.stream.threads (the configured
  // value is the fallback by definition) and strip the framework-only flag before it reaches
  // Kafka Streams.
  @Test
  public void dynamicWithoutResolverKeepsConfiguredValue() {
    final Object configuredThreads = sampleApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG);

    SampleApp dynamicApp =
        new SampleApp(ConfigClientFactory.getClient()) {
          @Override
          public Map<String, Object> getStreamsConfig(Config jobConfig) {
            Map<String, Object> properties = super.getStreamsConfig(jobConfig);
            properties.put(KafkaStreamsApp.DYNAMIC_NUM_STREAM_THREADS_CONFIG, true);
            return properties;
          }
        };

    dynamicApp.doInit();

    assertThat(dynamicApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG), is(configuredThreads));
    assertThat(
        dynamicApp.streamsConfig.containsKey(KafkaStreamsApp.DYNAMIC_NUM_STREAM_THREADS_CONFIG),
        is(false));
  }

  // Pattern-source topology: calculator returns OptionalInt.empty(). Configured num.stream.threads
  // flows through unchanged.
  @Test
  public void dynamicWithPatternSourceKeepsConfiguredValue() {
    final Object configuredThreads = sampleApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG);

    SampleApp dynamicApp =
        new SampleApp(ConfigClientFactory.getClient()) {
          @Override
          public Map<String, Object> getStreamsConfig(Config jobConfig) {
            Map<String, Object> properties = super.getStreamsConfig(jobConfig);
            properties.put(KafkaStreamsApp.DYNAMIC_NUM_STREAM_THREADS_CONFIG, true);
            return properties;
          }

          @Override
          public StreamsBuilder buildTopology(
              Map<String, Object> streamsConfig,
              StreamsBuilder streamsBuilder,
              Map<String, KStream<?, ?>> sourceStreams) {
            streamsBuilder.stream(
                    Pattern.compile("input-.*"), Consumed.with(Serdes.String(), Serdes.String()))
                .foreach((key, value) -> {});
            return streamsBuilder;
          }

          @Override
          protected Optional<StreamThreadsCountResolver> getStreamThreadsCountResolver() {
            return Optional.of(new StreamThreadsCountResolver(() -> 8));
          }
        };

    dynamicApp.doInit();

    assertThat(dynamicApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG), is(configuredThreads));
  }

  // Resolver throws → configured num.stream.threads flows through unchanged.
  @Test
  public void dynamicWithThrowingResolverKeepsConfiguredValue() {
    final Object configuredThreads = sampleApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG);

    SampleApp dynamicApp =
        new SampleApp(ConfigClientFactory.getClient()) {
          @Override
          public Map<String, Object> getStreamsConfig(Config jobConfig) {
            Map<String, Object> properties = super.getStreamsConfig(jobConfig);
            properties.put(KafkaStreamsApp.DYNAMIC_NUM_STREAM_THREADS_CONFIG, true);
            return properties;
          }

          @Override
          protected Optional<StreamThreadsCountResolver> getStreamThreadsCountResolver() {
            throw new RuntimeException("simulated wiring failure");
          }
        };

    dynamicApp.doInit();

    assertThat(dynamicApp.streamsConfig.get(NUM_STREAM_THREADS_CONFIG), is(configuredThreads));
  }
}
