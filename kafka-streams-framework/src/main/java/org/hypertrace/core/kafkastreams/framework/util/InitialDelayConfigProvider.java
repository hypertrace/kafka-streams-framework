package org.hypertrace.core.kafkastreams.framework.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class InitialDelayConfigProvider {

  private static final String INITIAL_DELAY = "initial.delay";
  private static final Duration DEFAULT_INITIAL_DELAY_MAJOR_VERSION_CHANGE = Duration.ofMinutes(6L);
  private static final Duration DEFAULT_INITIAL_DELAY_NO_MAJOR_VERSION_CHANGE =
      Duration.ofMillis(0L);
  private static final String SERVICE_VERSION = "SERVICE_VERSION";

  private static final InitialDelayConfigProvider instance = new InitialDelayConfigProvider();

  private InitialDelayConfigProvider() {}

  public static final InitialDelayConfigProvider getInstance() {
    return instance;
  }

  public Duration getInitialDelay(Map<String, Object> streamsConfig) {
    return getConfiguredInitialDelay(streamsConfig).orElse(getDefaultInitialDelay());
  }

  private Optional<Duration> getConfiguredInitialDelay(Map<String, Object> streamsConfig) {
    Optional<Object> initialDelayOpt = Optional.ofNullable(streamsConfig.get(INITIAL_DELAY));
    return initialDelayOpt.map(this::parseDuration);
  }

  private Duration getDefaultInitialDelay() {
    Optional<String> serviceVersion = Optional.ofNullable(System.getenv(SERVICE_VERSION));
    boolean majorVersionChange = isMajorVersionChange(serviceVersion);
    return majorVersionChange
        ? DEFAULT_INITIAL_DELAY_MAJOR_VERSION_CHANGE
        : DEFAULT_INITIAL_DELAY_NO_MAJOR_VERSION_CHANGE;
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
