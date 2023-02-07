package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.grpcutils.context.RequestContextConstants;
import org.hypertrace.partitioner.config.service.v1.GetPartitionerProfileRequest;
import org.hypertrace.partitioner.config.service.v1.PartitionerConfigServiceGrpc;
import org.hypertrace.partitioner.config.service.v1.PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub;

@Slf4j
public class PartitionerConfigServiceCachingClient implements PartitionerConfigServiceClient {
  private final PartitionerConfigServiceBlockingStub service;
  private final LoadingCache<String, WeightedGroupProfile> partitionerConfigProfileCache;

  public PartitionerConfigServiceCachingClient(Config config, GrpcChannelRegistry registry) {
    PartitionerConfigServiceClientConfig clientConfig =
        PartitionerConfigServiceClientConfig.from(config);
    Channel channel =
        registry.forPlaintextAddress(clientConfig.getHost(), clientConfig.getPort());
    this.service = PartitionerConfigServiceGrpc.newBlockingStub(channel);
    this.partitionerConfigProfileCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(clientConfig.getRefreshDuration())
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
    return partitionerConfigProfileCache.getUnchecked(profileName);
  }
}
