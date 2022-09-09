package org.hypertrace.core.kafkastreams.framework;

import java.util.Map;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.serviceframework.config.ConfigClient;

public class Service1 extends KafkaStreamsApp {

  static String INPUT_TOPIC = "service1_input";
  static String OUTPUT_TOPIC = "service1_output";

  public Service1(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams) {
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);
    return streamsBuilder;
  }

  public String getServiceName() {
    return "service1";
  }
}
