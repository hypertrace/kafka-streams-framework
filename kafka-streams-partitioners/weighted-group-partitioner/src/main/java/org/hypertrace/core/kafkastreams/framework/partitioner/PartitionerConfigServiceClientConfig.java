package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class PartitionerConfigServiceClientConfig {
  private static final String HOST_KEY = "host";
  private static final String PORT_KEY = "port";
  private static final String REFRESH_DURATION_KEY = "refresh.duration";
  private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofHours(1);

  String host;
  int port;
  Duration refreshDuration;

  public static PartitionerConfigServiceClientConfig from(Config config) {
    String host = config.getString(HOST_KEY);
    int port = config.getInt(PORT_KEY);
    Duration refreshDuration;

    // https://github.com/lightbend/config/blob/master/HOCON.md#duration-format
    // Examples: 3600000, 3600000ms, 3600s, 60m, 1h
    // Above examples indicates duration of 1 hours in different formats.
    if (config.hasPath(REFRESH_DURATION_KEY)) {
      refreshDuration = config.getDuration(REFRESH_DURATION_KEY);
    } else {
      refreshDuration = DEFAULT_REFRESH_DURATION;
    }
    return new PartitionerConfigServiceClientConfig(host, port, refreshDuration);
  }
}
