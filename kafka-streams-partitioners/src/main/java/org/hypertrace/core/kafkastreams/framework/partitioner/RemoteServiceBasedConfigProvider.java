package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.partitioner.config.service.v1.GetPartitionerProfileRequest;
import org.hypertrace.partitioner.config.service.v1.PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub;

@Slf4j
public class RemoteServiceBasedConfigProvider implements ConfigProvider {
  private final String profileName;
  private final LoadingCache<String, MultiLevelPartitionerConfig> partitionerConfigCache;

  public RemoteServiceBasedConfigProvider(
      PartitionerConfigServiceBlockingStub service, String profileName, Duration refreshInterval) {
    this.profileName = profileName;

    partitionerConfigCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(refreshInterval)
            .recordStats()
            .build(
                new CacheLoader<>() {
                  @Override
                  public MultiLevelPartitionerConfig load(String profileName) {
                    GetPartitionerProfileRequest getProfileRequest =
                        GetPartitionerProfileRequest.newBuilder()
                            .setProfileName(profileName)
                            .build();
                    try {
                      return new MultiLevelPartitionerConfig(
                          new RequestContext()
                              .call(
                                  () ->
                                      service
                                          .getPartitionerProfile(getProfileRequest)
                                          .getProfile()));
                    } catch (Exception e) {
                      throw new RuntimeException(
                          "Error occurred while fetching partitioner profile: " + profileName, e);
                    }
                  }
                });

    // Warm up the cache with initial configuration
    partitionerConfigCache.getUnchecked(profileName);
  }

  @Override
  public MultiLevelPartitionerConfig getConfig() {
    return partitionerConfigCache.getUnchecked(profileName);
  }
}
