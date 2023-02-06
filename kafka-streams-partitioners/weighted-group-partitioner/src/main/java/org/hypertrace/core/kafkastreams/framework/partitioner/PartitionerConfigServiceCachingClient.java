package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.grpcutils.context.RequestContextConstants;
import org.hypertrace.partitioner.config.service.v1.GetPartitionerProfileRequest;
import org.hypertrace.partitioner.config.service.v1.PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub;

@Slf4j
public class PartitionerConfigServiceCachingClient implements PartitionerConfigServiceClient {
  private final PartitionerConfigServiceBlockingStub service;
  private final LoadingCache<String, WeightedGroupProfile> partitionerConfigCache;

  public PartitionerConfigServiceCachingClient(Config config) {
    // TODO - implement
    service = null;
    partitionerConfigCache = null;
  }

  public PartitionerConfigServiceCachingClient(Config config, GrpcChannelRegistry registry) {
    // TODO - implement
    service = null;
    partitionerConfigCache = null;
  }

  public PartitionerConfigServiceCachingClient(
      PartitionerConfigServiceBlockingStub service, Duration refreshInterval) {
    this.service = service;
    partitionerConfigCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(refreshInterval)
            .recordStats()
            .build(CacheLoader.from(this::fetchPartitionerConfig));
  }

  private WeightedGroupProfile fetchPartitionerConfig(String profileName) {
    GetPartitionerProfileRequest getProfileRequest =
        GetPartitionerProfileRequest.newBuilder().setProfileName(profileName).build();
    try {
      return new WeightedGroupProfile(
          new RequestContext()
              .put(RequestContextConstants.REQUEST_ID_HEADER_KEY, UUID.randomUUID().toString())
              .call(() -> service.getPartitionerProfile(getProfileRequest).getProfile()));
    } catch (Exception e) {
      log.error("Error occurred while fetching profile: {}", profileName, e);
      throw e;
    }
  }

  @Override
  public WeightedGroupProfile getConfig(String profileName) {
    return partitionerConfigCache.getUnchecked(profileName);
  }
}
