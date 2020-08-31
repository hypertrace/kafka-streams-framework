package org.hypertrace.core.kafkastreams.framework;


import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;

public class SampleApp extends StreamsApp {

  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";
  static final String APP_ID = "testapp";

  protected SampleApp(Config jobConfig) {
    super(jobConfig);
  }

  @Override
  protected Topology buildTopology(Properties streamsConfig) {
    return retainWordsLongerThan5Letters();
  }

  @Override
  protected Properties getStreamsConfig(Config jobConfig) {
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
  protected Logger getLogger() {
    return null;
  }

  static Topology retainWordsLongerThan5Letters() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> stream = builder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);

    return builder.build();
  }
}