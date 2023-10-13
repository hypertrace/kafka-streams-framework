package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.BeforeEach;

class PartitionerConfigServiceCachingClientTest {
  private PartitionerConfigServiceCachingClient client;
  private GrpcChannelRegistry registry;

  @BeforeEach
  public void setUp() {
    GrpcChannelRegistry registry = new GrpcChannelRegistry();
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("host", "localhost");
    configMap.put("port", "10000");
    client = new PartitionerConfigServiceCachingClient(ConfigFactory.parseMap(configMap), registry);
  }

  public void getProfile() {
    client.getConfig("test-profile");
  }
}
