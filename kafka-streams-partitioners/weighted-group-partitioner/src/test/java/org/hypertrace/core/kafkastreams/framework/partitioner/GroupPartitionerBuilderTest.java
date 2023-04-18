package org.hypertrace.core.kafkastreams.framework.partitioner;

import static org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder.ENABLED_CONFIG_KEY;
import static org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder.GROUP_PARTITIONER_CONFIG_PREFIX;
import static org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder.GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX;
import static org.hypertrace.core.kafkastreams.framework.partitioner.PartitionerConfigServiceClientConfig.CACHE_DURATION_KEY;
import static org.hypertrace.core.kafkastreams.framework.partitioner.PartitionerConfigServiceClientConfig.HOST_KEY;
import static org.hypertrace.core.kafkastreams.framework.partitioner.PartitionerConfigServiceClientConfig.PORT_KEY;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

public class GroupPartitionerBuilderTest {
  private final GroupPartitionerBuilder<String, String> builder = new GroupPartitionerBuilder<>();
  private final BiFunction<String, String, String> memberIdExtractor = (key, value) -> key;
  private final GrpcChannelRegistry channelRegistry = new GrpcChannelRegistry();
  private final StreamPartitioner<String, String> delegatePartitioner =
      new RoundRobinPartitioner<>();

  @Test
  void buildPartitionerWhenGroupPartitionerDisabled() {
    Config appConfig = toConfig("dummy", "9000", "1d", "false");
    StreamPartitioner<String, String> resultPartitioner =
        builder.buildPartitioner("test-profile", appConfig, null, delegatePartitioner, null);
    assertSame(
        delegatePartitioner,
        resultPartitioner,
        "when group partitioner is disabled, builder should return delegate itself");
  }

  @Test
  void buildPartitionerWhenEnabledFlagNotConfigured() {
    Config appConfig = toConfig("dummy", "9000", "1d", null);
    StreamPartitioner<String, String> resultPartitioner =
        builder.buildPartitioner("test-profile", appConfig, null, delegatePartitioner, null);
    assertSame(
        delegatePartitioner,
        resultPartitioner,
        "when group partitioner is not explicitly enabled, builder should return delegate itself");
  }

  @Test
  void buildPartitionerWhenGroupPartitionerEnabled() {
    Config appConfig = toConfig("host-1", "8000", "1h", "true");
    StreamPartitioner<String, String> resultPartitioner =
        builder.buildPartitioner(
            "test-profile", appConfig, memberIdExtractor, delegatePartitioner, channelRegistry);
    assertInstanceOf(WeightedGroupPartitioner.class, resultPartitioner);
  }

  @Test
  void buildPartitionerWithMinimalConfig() {
    Config appConfig = toConfig("host-1", "8000", null, "true");
    StreamPartitioner<String, String> resultPartitioner =
        builder.buildPartitioner(
            "test-profile", appConfig, memberIdExtractor, delegatePartitioner, channelRegistry);
    assertInstanceOf(WeightedGroupPartitioner.class, resultPartitioner);
  }

  private Config toConfig(String host, String port, String cacheDuration, String enabled) {
    Map<String, String> configMap = Maps.newHashMap();
    putIfNotNull(configMap, enabled, GROUP_PARTITIONER_CONFIG_PREFIX, ENABLED_CONFIG_KEY);
    putIfNotNull(
        configMap,
        host,
        GROUP_PARTITIONER_CONFIG_PREFIX,
        GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX,
        HOST_KEY);
    putIfNotNull(
        configMap,
        port,
        GROUP_PARTITIONER_CONFIG_PREFIX,
        GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX,
        PORT_KEY);
    putIfNotNull(
        configMap,
        cacheDuration,
        GROUP_PARTITIONER_CONFIG_PREFIX,
        GROUP_PARTITIONER_CONFIG_SERVICE_PREFIX,
        CACHE_DURATION_KEY);
    return ConfigFactory.parseMap(configMap);
  }

  private void putIfNotNull(Map<String, String> configMap, String value, String... keyElements) {
    configMap.put(Joiner.on(".").join(keyElements), value);
  }
}
