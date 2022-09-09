package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.util.Map;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.constants.KafkaStreamsAppConstants;
import org.hypertrace.core.serviceframework.config.ConfigClient;

public class SampleApp extends KafkaStreamsApp {
  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";

  public SampleApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInitForConsolidatedKStreamApp(Config subTopologyJobConfig) {}

  @Override
  protected void doCleanUpForConsolidatedKStreamApp() {}

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams) {
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);
    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return KafkaStreamsAppConstants.JOB_CONFIG;
  }

  @Override
  public String getServiceName() {
    return "SampleApp";
  }
}
