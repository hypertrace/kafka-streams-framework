package org.hypertrace.core.kafkastreams.framework.punctuators;

import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;

import com.typesafe.config.Config;
import lombok.Getter;

@Getter
public class ThrottledPunctuatorConfig {
  private static final String YIELD_CONFIG_SUFFIX = ".yield.ms";
  private static final String WINDOW_CONFIG_SUFFIX = ".window.ms";
  private static final long DEFAULT_WINDOW_MS = 1;
  private static final double DEFAULT_YIELD_SESSION_TIMEOUT_RATIO = 1 / 5.0;
  private final long yieldMs;
  private final long windowMs;

  public ThrottledPunctuatorConfig(Config kafkaStreamsConfig, String punctuatorName) {
    if (kafkaStreamsConfig.hasPath(punctuatorName + YIELD_CONFIG_SUFFIX)) {
      this.yieldMs = kafkaStreamsConfig.getLong(punctuatorName + YIELD_CONFIG_SUFFIX);
    } else {
      // when not configured, set to 20% of session timeout.
      this.yieldMs =
          (long)
              (kafkaStreamsConfig.getLong(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG))
                  * DEFAULT_YIELD_SESSION_TIMEOUT_RATIO);
    }
    if (kafkaStreamsConfig.hasPath(punctuatorName + WINDOW_CONFIG_SUFFIX)) {
      this.windowMs = kafkaStreamsConfig.getLong(punctuatorName + WINDOW_CONFIG_SUFFIX);
    } else {
      this.windowMs = DEFAULT_WINDOW_MS;
    }
  }
}
