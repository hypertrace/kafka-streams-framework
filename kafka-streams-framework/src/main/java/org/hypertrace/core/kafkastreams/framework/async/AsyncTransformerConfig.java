package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_TRANSFORMER_BATCH_SIZE;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_TRANSFORMER_COMMIT_INTERVAL;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Getter;

@Getter
public class AsyncTransformerConfig {
  private static final String COMMIT_INTERVAL_CONFIG_KEY = "commitIntervalMs";
  private static final String MAX_BATCH_SIZE_CONFIG_KEY = "maxBatchSize";
  private static final String TRANSFORMERS_CONFIG_KEY = "async.transformers";
  private final int maxBatchSize;
  private final Duration commitIntervalMs;

  AsyncTransformerConfig(int maxBatchSize, int commitIntervalMs) {
    this.maxBatchSize = maxBatchSize;
    this.commitIntervalMs = Duration.ofMillis(commitIntervalMs);
  }

  public static AsyncTransformerConfig buildWith(Config config, String transformerName) {
    Config transformersConfig =
        config.hasPath(TRANSFORMERS_CONFIG_KEY) ? config.getConfig(TRANSFORMERS_CONFIG_KEY) : null;
    if (transformersConfig != null && transformersConfig.hasPath(transformerName)) {
      Config transformerConfig = transformersConfig.getConfig(transformerName);
      int batchSize =
          transformerConfig.hasPath(MAX_BATCH_SIZE_CONFIG_KEY)
              ? transformerConfig.getInt(MAX_BATCH_SIZE_CONFIG_KEY)
              : DEFAULT_ASYNC_TRANSFORMER_BATCH_SIZE;
      int commitInterval =
          transformerConfig.hasPath(COMMIT_INTERVAL_CONFIG_KEY)
              ? transformerConfig.getInt(COMMIT_INTERVAL_CONFIG_KEY)
              : DEFAULT_ASYNC_TRANSFORMER_COMMIT_INTERVAL;
      return new AsyncTransformerConfig(batchSize, commitInterval);
    }

    return new AsyncTransformerConfig(
        DEFAULT_ASYNC_TRANSFORMER_BATCH_SIZE, DEFAULT_ASYNC_TRANSFORMER_COMMIT_INTERVAL);
  }
}
