package org.hypertrace.core.kafkastreams.framework.partitioner;

import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.partitioner.config.service.v1.GetPartitionerProfileRequest;
import org.hypertrace.partitioner.config.service.v1.PartitionerConfigServiceGrpc.PartitionerConfigServiceBlockingStub;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

@Slf4j
public class RemoteServiceBasedConfigProvider implements ConfigProvider {
  private final PartitionerConfigServiceBlockingStub configService;
  private final String profileName;
  private final Duration refreshInterval;

  private PartitionerProfile profile;
  private Instant lastUpdated;

  public RemoteServiceBasedConfigProvider(
      PartitionerConfigServiceBlockingStub service, String profileName, Duration refreshInterval) {
    this.configService = service;
    this.profileName = profileName;
    this.profile = getPartitionerProfile(service, profileName, false);
    this.lastUpdated = Instant.now();
    this.refreshInterval = refreshInterval;
  }

  @Override
  public MultiLevelPartitionerConfig getConfig() {
    Instant now = Instant.now();
    if (now.compareTo(lastUpdated.plus(refreshInterval)) > 0)
      this.profile = getPartitionerProfile(configService, profileName, false);
    return null;
  }

  private PartitionerProfile getPartitionerProfile(
      PartitionerConfigServiceBlockingStub service, String profileName, boolean ignoreErrors) {
    GetPartitionerProfileRequest getProfileRequest =
        GetPartitionerProfileRequest.newBuilder().setProfileName(profileName).build();
    try {
      return service.getPartitionerProfile(getProfileRequest).getProfile();
    } catch (Exception e) {
      if (!ignoreErrors) throw e;
      else {
        log.error(
            "Will reuse the existing profile. Error occurred while fetching partitioner profile: {}",
            profileName);
        return profile;
      }
    }
  }
}
