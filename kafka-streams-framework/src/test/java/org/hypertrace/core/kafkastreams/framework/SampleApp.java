package org.hypertrace.core.kafkastreams.framework;


import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;

public class SampleApp extends KafkaStreamsApp {

  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";
  static final String APP_ID = "testapp";

  public SampleApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Properties streamsConfig, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams) {
    return retainWordsLongerThan5Letters(streamsBuilder);
  }

  @Override
  public Properties getStreamsConfig(Config jobConfig) {
    Properties config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    return config;
  }

  @Override
  public Logger getLogger() {
    return null;
  }

  @Override
  public List<String> getInputTopics(Properties streamsConfig) {
    return Arrays.asList(INPUT_TOPIC);
  }

  @Override
  public List<String> getOutputTopics(Properties streamsConfig) {
    return Arrays.asList(OUTPUT_TOPIC);
  }

  @Override
  public String getServiceName() {
    return "SampleApp";
  }


  static StreamsBuilder retainWordsLongerThan5Letters(StreamsBuilder streamsBuilder) {
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);

    return streamsBuilder;
  }
}