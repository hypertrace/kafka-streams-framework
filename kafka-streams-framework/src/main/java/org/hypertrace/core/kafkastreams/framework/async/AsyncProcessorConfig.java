package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_PROCESSOR_BATCH_SIZE;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_PROCESSOR_COMMIT_INTERVAL;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Getter;

@Getter
public class AsyncProcessorConfig {
  private static final String COMMIT_INTERVAL_CONFIG_KEY = "commitIntervalMs";
  private static final String MAX_BATCH_SIZE_CONFIG_KEY = "maxBatchSize";
  private static final String PROCESSORS_CONFIG_KEY = "async.processors";
  private final int maxBatchSize;
  private final Duration commitIntervalMs;

  AsyncProcessorConfig(int maxBatchSize, int commitIntervalMs) {
    this.maxBatchSize = maxBatchSize;
    this.commitIntervalMs = Duration.ofMillis(commitIntervalMs);
  }

  public static AsyncProcessorConfig buildWith(Config config, String processorName) {
    Config processorsConfig =
        config.hasPath(PROCESSORS_CONFIG_KEY) ? config.getConfig(PROCESSORS_CONFIG_KEY) : null;
    if (processorsConfig != null && processorsConfig.hasPath(processorName)) {
      Config processorConfig = processorsConfig.getConfig(processorName);
      int batchSize =
          processorConfig.hasPath(MAX_BATCH_SIZE_CONFIG_KEY)
              ? processorConfig.getInt(MAX_BATCH_SIZE_CONFIG_KEY)
              : DEFAULT_ASYNC_PROCESSOR_BATCH_SIZE;
      int commitInterval =
          processorConfig.hasPath(COMMIT_INTERVAL_CONFIG_KEY)
              ? processorConfig.getInt(COMMIT_INTERVAL_CONFIG_KEY)
              : DEFAULT_ASYNC_PROCESSOR_COMMIT_INTERVAL;
      return new AsyncProcessorConfig(batchSize, commitInterval);
    }

    return new AsyncProcessorConfig(
        DEFAULT_ASYNC_PROCESSOR_BATCH_SIZE, DEFAULT_ASYNC_PROCESSOR_COMMIT_INTERVAL);
  }
}
