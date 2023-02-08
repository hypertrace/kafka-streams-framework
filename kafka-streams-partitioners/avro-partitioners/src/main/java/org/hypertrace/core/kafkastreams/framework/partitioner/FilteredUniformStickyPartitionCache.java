package org.hypertrace.core.kafkastreams.framework.partitioner;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;

/**
 * An internal class that implements a cache used for filtered sticky partitioning behavior. The
 * cache tracks the current sticky partition for any given topic.
 */
public class FilteredUniformStickyPartitionCache extends StickyPartitionCache {
  private final String configRegex = "filtered\\.partitioner\\.(.*?)\\.excluded\\.partitions";
  private final Pattern pattern = Pattern.compile(configRegex);
  private final Map<String, Set<Integer>> excludedTopicPartitions = Maps.newConcurrentMap();

  public void configure(Map<String, ?> configs) {
    configs.entrySet().stream()
        .filter(config -> pattern.matcher(config.getKey()).find())
        .forEach(
            config -> {
              Matcher matcher = pattern.matcher(config.getKey());
              matcher.find();
              String topic = matcher.group(1);
              String excludePartitionsStr = (String) config.getValue();
              Set<Integer> excludedPartitions =
                  Splitter.on(",")
                      .trimResults()
                      .splitToStream(excludePartitionsStr)
                      .map(Integer::valueOf)
                      .collect(Collectors.toSet());
              excludedTopicPartitions.put(topic, excludedPartitions);
            });
  }

  public int nextPartition(String topic, Cluster cluster, int prevPartition) {
    int newPartition = super.nextPartition(topic, cluster, prevPartition);
    if (excludedTopicPartitions.containsKey(topic)) {
      Set<Integer> partitionsExcluded = excludedTopicPartitions.get(topic);
      while (partitionsExcluded.contains(newPartition)) {
        newPartition = super.nextPartition(topic, cluster, newPartition);
      }
    }
    return newPartition;
  }
}
