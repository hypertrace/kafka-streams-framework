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
public class WeightedGroupProfile {
  final String name;
  final Map<String, WeightedGroup> groupByMember;
  final WeightedGroup defaultGroup;

  @Value
  static class WeightedGroup {
    String groupName;
    // This represents some range between 0-1 (e.g. 0.6-0.8) that specifies this group's share
    double normalizedFractionalStart;
    double normalizedFractionalEnd;
  }

  public WeightedGroupProfile(PartitionerProfile profile) {
    this.name = profile.getName();
    double defaultGroupWeight = profile.getDefaultGroupWeight();
    List<PartitionerGroup> groups = profile.getGroupsList();

    double totalWeight =
        groups.stream().map(PartitionerGroup::getWeight).reduce(0, Integer::sum)
            + defaultGroupWeight;

    AtomicDouble weightConsumedSoFar = new AtomicDouble(0);

    this.groupByMember =
        groups.stream()
            .flatMap(
                groupConfig ->
                    buildEntriesForEachMember(groupConfig, weightConsumedSoFar, totalWeight))
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    this.defaultGroup =
        new WeightedGroup(
            "[[default]]",
            weightConsumedSoFar.get(),
            weightConsumedSoFar.addAndGet(defaultGroupWeight / totalWeight));

    log.info(
        "partitioner default group config - weight range: {}, range end: {}",
        defaultGroup.getNormalizedFractionalStart(),
        defaultGroup.getNormalizedFractionalEnd());
  }

  public WeightedGroup getGroupByMember(String memberId) {
    return groupByMember.getOrDefault(memberId, defaultGroup);
  }

  private static Stream<Entry<String, WeightedGroup>> buildEntriesForEachMember(
      PartitionerGroup groupConfig, AtomicDouble weightConsumedSoFar, double totalWeight) {
    double groupWeightStart = weightConsumedSoFar.get();
    double groupWeightEnd = weightConsumedSoFar.addAndGet(groupConfig.getWeight() / totalWeight);
    WeightedGroup weightedGroup =
        new WeightedGroup(groupConfig.getName(), groupWeightStart, groupWeightEnd);

    log.info(
        "partitioner group config - group: {}, range start: {}, range end: {}, members: {}",
        groupConfig.getName(),
        weightedGroup.getNormalizedFractionalStart(),
        weightedGroup.getNormalizedFractionalEnd(),
        groupConfig.getMemberIdsList());

    return groupConfig.getMemberIdsList().stream()
        .map(memberId -> Map.entry(memberId, weightedGroup));
  }
}
