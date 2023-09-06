package org.hypertrace.core.kafkastreams.framework.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class InitialDelayParser {

  private static final String INITIAL_DELAY = "initial.delay";
  private static final Duration INITIAL_DELAY_VERSION_CHANGE = Duration.ofMinutes(6L);
  private static final Duration INITIAL_DELAY_NO_VERSION_CHANGE = Duration.ofMillis(0L);
  private static final String SERVICE_VERSION = "SERVICE_VERSION";

  private static final InitialDelayParser instance = new InitialDelayParser();

  private InitialDelayParser() {}

  public static final InitialDelayParser getInstance() {
    return instance;
  }

  public Duration getInitialDelay(Map<String, Object> streamsConfig) {
    Optional<String> serviceVersion = Optional.ofNullable(System.getenv(SERVICE_VERSION));
    boolean majorVersionChange = isMajorVersionChange(serviceVersion);
    Duration defaultInitialDelay =
        majorVersionChange ? INITIAL_DELAY_VERSION_CHANGE : INITIAL_DELAY_NO_VERSION_CHANGE;
    return getConfiguredInitialDelay(streamsConfig).orElse(defaultInitialDelay);
  }

  private Optional<Duration> getConfiguredInitialDelay(Map<String, Object> streamsConfig) {
    Optional<Object> initialDelayOpt = Optional.ofNullable(streamsConfig.get(INITIAL_DELAY));
    return initialDelayOpt.map(this::parseDuration);
  }

  private Duration parseDuration(Object initialDelayObj) {
    Properties properties = new Properties();
    properties.put(INITIAL_DELAY, initialDelayObj);
    Config parsedConfig = ConfigFactory.parseProperties(properties);
    return parsedConfig.getDuration(INITIAL_DELAY);
  }

  private boolean isMajorVersionChange(Optional<String> serviceVersion) {
    if (serviceVersion.isPresent()) {
      String[] version = serviceVersion.get().split("\\.");
      if (version.length == 3) {
        return version[1].equals("0") && version[2].equals("0");
      }
    }
    return false;
  }
}
