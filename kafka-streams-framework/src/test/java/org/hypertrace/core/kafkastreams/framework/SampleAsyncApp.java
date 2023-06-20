package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.hypertrace.core.kafkastreams.framework.async.AsyncTransformer;
import org.hypertrace.core.kafkastreams.framework.async.AsyncTransformerConfig;
import org.hypertrace.core.kafkastreams.framework.async.ExecutorFactory;
import org.hypertrace.core.kafkastreams.framework.constants.KafkaStreamsAppConstants;
import org.hypertrace.core.serviceframework.config.ConfigClient;

@Slf4j
public class SampleAsyncApp extends KafkaStreamsApp {
  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";

  public SampleAsyncApp(ConfigClient configClient) {
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

    Config kafkaStreamsConfig = configClient.getConfig().getConfig(KAFKA_STREAMS_CONFIG_KEY);
    KStream<String, String> transform =
        stream.transform(
            () ->
                new SlowTransformer(
                    ExecutorFactory.getExecutorSupplier(kafkaStreamsConfig),
                    AsyncTransformerConfig.buildWith(kafkaStreamsConfig, "slow.transformer")));
    transform.process(LoggingProcessor::new);
    transform.to(OUTPUT_TOPIC);
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

@Slf4j
class SlowTransformer extends AsyncTransformer<String, String, String, String> {

  public SlowTransformer(
      Supplier<Executor> executorSupplier, AsyncTransformerConfig asyncTransformerConfig) {
    super(executorSupplier, asyncTransformerConfig);
  }

  @Override
  protected void doInit(Map<String, Object> appConfigs) {
    // no-op
  }

  @SneakyThrows
  @Override
  public List<KeyValue<String, String>> asyncTransform(String key, String value) {
    if(!key.startsWith("key")) {
      return null;
    }
    log.info("transforming - key: {}, value: {}", key, value);
    Thread.sleep(25);
    return List.of(KeyValue.pair("out:" + key, "out:" + value));
  }
}

@Slf4j
class LoggingProcessor implements Processor<String, String, Void, Void> {

  @Override
  public void process(Record<String, String> record) {
    log.info("received - key: {}, value: {}", record.key(), record.value());
  }
}
