package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.util.concurrent.AtomicDouble;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

@Slf4j
@Value
class MultiLevelPartitionerConfig {
  PartitionGroupConfig defaultGroupConfig;
  Map<String, PartitionGroupConfig> groupConfigByMember;

  @Value
  static class PartitionGroupConfig {
    // This represents some range between 0-1 (e.g. 0.6-0.8) that specifies this group's share
    double normalizedFractionalStart;
    double normalizedFractionalEnd;
  }

  public MultiLevelPartitionerConfig(PartitionerProfile profile) {
    double defaultWeight = profile.getDefaultGroupWeight();

    // Sort groups by name for consistent ordering
    List<PartitionerGroup> groupConfigs = profile.getGroupsList();

    double totalWeight =
        defaultWeight
            + groupConfigs.stream().map(PartitionerGroup::getWeight).reduce(0, Integer::sum);
    AtomicDouble weightConsumedSoFar = new AtomicDouble();
    this.defaultGroupConfig =
        new PartitionGroupConfig(
            weightConsumedSoFar.get(), weightConsumedSoFar.addAndGet(defaultWeight / totalWeight));

    this.groupConfigByMember =
        groupConfigs.stream()
            .map(
                groupConfig ->
                    buildKeyValueMapForConfig(
                        groupConfig,
                        new PartitionGroupConfig(
                            weightConsumedSoFar.get(),
                            weightConsumedSoFar.addAndGet(groupConfig.getWeight() / totalWeight))))
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    log.info("partitioner config: default partition weight: {}", defaultGroupConfig);
    log.info("partitioner config: partitioner groups: {}", groupConfigByMember);
  }

  private static Map<String, PartitionGroupConfig> buildKeyValueMapForConfig(
      PartitionerGroup groupConfig, PartitionGroupConfig partitionGroupConfig) {
    return groupConfig.getMemberIdsList().stream()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), unused -> partitionGroupConfig));
  }
}
