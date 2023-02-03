package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.util.concurrent.AtomicDouble;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.partitioner.config.service.v1.PartitionerGroup;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

@Slf4j
public class MultiLevelPartitionerConfig {
  final String profileName;
  final Map<String, PartitionGroupInfo> groupInfoByMember;
  final PartitionGroupInfo defaultGroupInfo;

  @Value
  static class PartitionGroupInfo {
    String groupName;
    // This represents some range between 0-1 (e.g. 0.6-0.8) that specifies this group's share
    double normalizedFractionalStart;
    double normalizedFractionalEnd;
  }

  public MultiLevelPartitionerConfig(PartitionerProfile profile) {
    this.profileName = profile.getName();
    double defaultGroupWeight = profile.getDefaultGroupWeight();
    List<PartitionerGroup> groups = profile.getGroupsList();

    double totalWeight =
        groups.stream().map(PartitionerGroup::getWeight).reduce(0, Integer::sum)
            + defaultGroupWeight;

    AtomicDouble weightConsumedSoFar = new AtomicDouble(0);

    this.groupInfoByMember =
        groups.stream()
            .flatMap(
                groupConfig ->
                    buildEntriesForEachMember(groupConfig, weightConsumedSoFar, totalWeight))
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    this.defaultGroupInfo =
        new PartitionGroupInfo(
            null,
            weightConsumedSoFar.get(),
            weightConsumedSoFar.addAndGet(defaultGroupWeight / totalWeight));

    log.info(
        "partitioner default group config - weight range: {}, range end: {}",
        defaultGroupInfo.getNormalizedFractionalStart(),
        defaultGroupInfo.getNormalizedFractionalEnd());
  }

  public PartitionGroupInfo getGroupInfoByMember(String memberId) {
    return groupInfoByMember.getOrDefault(memberId, defaultGroupInfo);
  }

  private static Stream<Entry<String, PartitionGroupInfo>> buildEntriesForEachMember(
      PartitionerGroup groupConfig, AtomicDouble weightConsumedSoFar, double totalWeight) {
    double groupWeightStart = weightConsumedSoFar.get();
    double groupWeightEnd = weightConsumedSoFar.addAndGet(groupConfig.getWeight() / totalWeight);
    PartitionGroupInfo partitionGroupInfo =
        new PartitionGroupInfo(groupConfig.getName(), groupWeightStart, groupWeightEnd);

    log.info(
        "partitioner group config - group: {}, range start: {}, range end: {}, members: {}",
        groupConfig.getName(),
        partitionGroupInfo.getNormalizedFractionalStart(),
        partitionGroupInfo.getNormalizedFractionalEnd(),
        groupConfig.getMemberIdsList());

    return groupConfig.getMemberIdsList().stream()
        .map(memberId -> Map.entry(memberId, partitionGroupInfo));
  }
}
