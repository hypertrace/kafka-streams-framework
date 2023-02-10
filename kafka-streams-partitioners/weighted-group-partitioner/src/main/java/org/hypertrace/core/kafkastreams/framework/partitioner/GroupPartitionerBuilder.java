package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

/**
 * Helper class to build the group partitioner when enabled. Returns delegate partitioner when group
 * partitioner is disabled.
 */
public class GroupPartitionerBuilder<K, V> {
  static final String GROUP_PARTITIONER_CONFIG_PREFIX = "group.partitioner";
  static final String GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX = "service";
  static final String ENABLED_CONFIG_KEY = "enabled";

  public StreamPartitioner<K, V> buildPartitioner(
      String profile,
      Config appConfig,
      BiFunction<K, V, String> memberIdExtractor,
      StreamPartitioner<K, V> delegatePartitioner,
      GrpcChannelRegistry channelRegistry) {
    String enabledConfigPath =
        ConfigUtil.joinPath(GROUP_PARTITIONER_CONFIG_PREFIX, ENABLED_CONFIG_KEY);
    // Use group partitioner only when explicitly enabled.
    if (appConfig.hasPath(enabledConfigPath) && appConfig.getBoolean(enabledConfigPath)) {
      String serviceConfigPath =
          ConfigUtil.joinPath(
              GROUP_PARTITIONER_CONFIG_PREFIX, GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX);
      PartitionerConfigServiceCachingClient configServiceClient =
          new PartitionerConfigServiceCachingClient(
              appConfig.getConfig(serviceConfigPath), channelRegistry);
      return new WeightedGroupPartitioner<>(
          profile, configServiceClient, memberIdExtractor, delegatePartitioner);
    }
    // All other cases, just return the delegate partitioner
    return delegatePartitioner;
  }
}