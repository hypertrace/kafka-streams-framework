package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicDouble;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
class AvroFieldValuePartitionerConfig {
  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  static final String PARTITIONER_CONFIG_PREFIX = "avro.field.value.partitioner";

  static final String TOPICS_CONFIG_PREFIX = "topics";
  static final String FIELD_NAME = "field.name";
  static final String EXCLUDED_PARTITIONS = "excluded.partitions";
  static final String DEFAULT_GROUP_WEIGHT = "default.group.weight";

  static final String GROUPS_CONFIG_PREFIX = "groups";
  static final String GROUP_MEMBERS = "members";
  static final String GROUP_WEIGHT = "weight";

  Map<String, String> fieldNameByTopic;
  Map<String, Set<Integer>> excludedPartitionsByTopic;
  PartitionGroupConfig defaultGroupConfig;
  Map<String, PartitionGroupConfig> groupConfigByMember;

  @Value
  static class PartitionGroupConfig {
    // This represents some range between 0-1 (e.g. 0.6-0.8) that specifies this group's share
    double normalizedFractionalStart;
    double normalizedFractionalEnd;
  }

  public AvroFieldValuePartitionerConfig(Map<String, ?> streamConfigMap) {
    final Map<String, String> fieldNameByTopic = Maps.newHashMap();
    final Map<String, Set<Integer>> excludedPartitionsByTopic = Maps.newHashMap();

    Map<String, Object> partitionerConfigProps =
        streamConfigMap.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(PARTITIONER_CONFIG_PREFIX))
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    Config partitionerConfig =
        ConfigFactory.parseMap(partitionerConfigProps).getConfig(PARTITIONER_CONFIG_PREFIX);

    Config topicsConfig = partitionerConfig.getConfig(TOPICS_CONFIG_PREFIX);
    topicsConfig
        .root()
        .keySet()
        .forEach(
            topic -> {
              Config topicConfig = topicsConfig.getConfig(topic);
              fieldNameByTopic.put(topic, topicConfig.getString(FIELD_NAME));
              String excludedPartitionsStr =
                  topicConfig.hasPath(EXCLUDED_PARTITIONS)
                      ? topicConfig.getString(EXCLUDED_PARTITIONS)
                      : "";
              Set<Integer> excludedPartitions =
                  SPLITTER
                      .splitToStream(excludedPartitionsStr)
                      .map(Integer::valueOf)
                      .collect(Collectors.toSet());
              excludedPartitionsByTopic.put(topic, excludedPartitions);
            });

    this.fieldNameByTopic = Map.copyOf(fieldNameByTopic);
    this.excludedPartitionsByTopic = Map.copyOf(excludedPartitionsByTopic);
    double defaultWeight = partitionerConfig.getDouble(DEFAULT_GROUP_WEIGHT);

    Config groupsConfig =
        partitionerConfig.hasPath(GROUPS_CONFIG_PREFIX)
            ? partitionerConfig.getConfig(GROUPS_CONFIG_PREFIX)
            : ConfigFactory.empty();

    // Sort groups by name for consistent ordering
    List<Config> groupConfigs =
        groupsConfig.root().keySet().stream()
            .sorted()
            .map(groupsConfig::getConfig)
            .collect(Collectors.toUnmodifiableList());

    double totalWeight =
        defaultWeight
            + groupConfigs.stream()
                .map(groupConfig -> groupConfig.getDouble(GROUP_WEIGHT))
                .reduce(0d, Double::sum);
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
                            weightConsumedSoFar.addAndGet(
                                groupConfig.getDouble(GROUP_WEIGHT) / totalWeight))))
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  private static Map<String, PartitionGroupConfig> buildKeyValueMapForConfig(
      Config groupConfig, PartitionGroupConfig partitionGroupConfig) {
    return SPLITTER.splitToList(groupConfig.getString(GROUP_MEMBERS)).stream()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), unused -> partitionGroupConfig));
  }
}
