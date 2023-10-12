package org.hypertrace.core.kafkastreams.framework.punctuators;

import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;

import com.typesafe.config.Config;

public class ThrottledPunctuatorConfig {
  private static final String YIELD_CONFIG_SUFFIX = ".yield.ms";
  private static final String WINDOW_CONFIG_SUFFIX = ".window.ms";
  private static final long DEFAULT_WINDOW_MS = 1;
  private static final long DEFAULT_YIELD_SESSION_TIMEOUT_FACTOR = 5;
  private final long yieldMs;
  private final long windowMs;

  public ThrottledPunctuatorConfig(Config kafkaStreamsConfig, String throttledPunctuatorName) {
    if (kafkaStreamsConfig.hasPath(throttledPunctuatorName + YIELD_CONFIG_SUFFIX)) {
      this.yieldMs = kafkaStreamsConfig.getLong(throttledPunctuatorName + YIELD_CONFIG_SUFFIX);
    } else {
      this.yieldMs =
          kafkaStreamsConfig.getLong(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG))
              / DEFAULT_YIELD_SESSION_TIMEOUT_FACTOR;
    }
    if (kafkaStreamsConfig.hasPath(throttledPunctuatorName + WINDOW_CONFIG_SUFFIX)) {
      this.windowMs = kafkaStreamsConfig.getLong(throttledPunctuatorName + WINDOW_CONFIG_SUFFIX);
    } else {
      this.windowMs = DEFAULT_WINDOW_MS;
    }
  }

  public long getYieldMs() {
    return yieldMs;
  }

  public long getWindowMs() {
    return windowMs;
  }
}
