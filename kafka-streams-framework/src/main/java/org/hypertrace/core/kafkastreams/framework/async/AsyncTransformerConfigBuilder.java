package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp.KAFKA_STREAMS_CONFIG_KEY;

import com.typesafe.config.Config;
import java.time.Duration;

public class AsyncTransformerConfigBuilder {
  private final Config jobConfig;
  private static final String COMMIT_INTERVAL_CONFIG_KEY = "commitIntervalMs";
  private static final String MAX_BATCH_SIZE_CONFIG_KEY = "maxBatchSize";
  private static final String TRANSFORMERS_CONFIG_KEY = "async.transformers";

  public AsyncTransformerConfigBuilder(Config jobConfig) {
    this.jobConfig = jobConfig;
  }

  public int maxBatchSize(String transformerName) {
    return getTransformerConfig(transformerName).getInt(MAX_BATCH_SIZE_CONFIG_KEY);
  }

  public Duration commitInterval(String transformerName) {
    return Duration.ofMillis(
        getTransformerConfig(transformerName).getInt(COMMIT_INTERVAL_CONFIG_KEY));
  }

  private Config getTransformerConfig(String transformerName) {
    return jobConfig
        .getConfig(KAFKA_STREAMS_CONFIG_KEY)
        .getConfig(TRANSFORMERS_CONFIG_KEY)
        .getConfig(transformerName);
  }
}
