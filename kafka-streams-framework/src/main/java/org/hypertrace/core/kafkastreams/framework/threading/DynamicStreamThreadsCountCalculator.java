package org.hypertrace.core.kafkastreams.framework.threading;

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes a per-instance {@code num.stream.threads} value from a topology and the partition count
 * of every source topic.
 *
 * <p>For each sub-topology the maximum partition count across its source topics is the number of
 * stream tasks. Summing across sub-topologies and dividing by the replica count yields the threads
 * each instance should run to keep all tasks active without idle threads.
 *
 * <p>Returns {@link OptionalInt#empty()} when the topology contains a regex/pattern subscription
 * ({@link Source#topicPattern()}) — those sub-topologies cannot be enumerated against the broker
 * up-front, so dynamic sizing would silently under-count tasks. The caller falls back to its
 * configured default in that case.
 */
public class DynamicStreamThreadsCountCalculator {

  private static final long DESCRIBE_TOPICS_TIMEOUT_MILLIS = Duration.ofSeconds(5).toMillis();
  private static final Logger logger =
      LoggerFactory.getLogger(DynamicStreamThreadsCountCalculator.class);

  private static Set<String> sourceTopicsOf(final Subtopology subtopology) {
    return subtopology.nodes().stream()
        .filter(node -> node instanceof Source)
        .map(node -> (Source) node)
        .flatMap(source -> source.topicSet().stream())
        .collect(toUnmodifiableSet());
  }

  private static boolean hasPatternSource(final Subtopology subtopology) {
    return subtopology.nodes().stream()
        .filter(node -> node instanceof Source)
        .map(node -> (Source) node)
        .anyMatch(source -> source.topicPattern() != null);
  }

  public OptionalInt compute(
      final Topology topology, final AdminClient adminClient, final int replicas) {
    if (replicas <= 0) {
      throw new IllegalArgumentException("replicas must be positive, got " + replicas);
    }

    final TopologyDescription description = topology.describe();

    // Bail out if any sub-topology subscribes via regex — topicSet() is empty for those, so
    // dynamic sizing would silently under-count tasks. The caller substitutes its fallback.
    final boolean anyPatternSource =
        description.subtopologies().stream()
            .anyMatch(DynamicStreamThreadsCountCalculator::hasPatternSource);
    if (anyPatternSource) {
      logger.warn(
          "Topology contains a regex/pattern source; dynamic num.stream.threads is not supported. "
              + "Caller will fall back to its configured default.");
      return OptionalInt.empty();
    }

    final Set<String> sourceTopics =
        description.subtopologies().stream()
            .flatMap(subtopology -> sourceTopicsOf(subtopology).stream())
            .collect(toUnmodifiableSet());

    final Map<String, Integer> partitionsByTopic = describePartitions(adminClient, sourceTopics);

    int totalTasks = 0;
    int subtopologyCount = 0;
    for (final Subtopology subtopology : description.subtopologies()) {
      subtopologyCount++;
      final Set<String> subtopologyTopics = sourceTopicsOf(subtopology);

      final int tasksForSubtopology =
          subtopologyTopics.stream()
              .mapToInt(topic -> partitionsByTopic.getOrDefault(topic, 0))
              .max()
              .orElse(0);

      if (tasksForSubtopology == 0) {
        logger.warn(
            "Sub-topology has no resolvable partitions; topics={}. Pod restart will be needed once topics exist.",
            subtopologyTopics);
      }
      totalTasks += tasksForSubtopology;
    }

    final int threads = totalTasks == 0 ? 1 : (int) Math.ceil((double) totalTasks / replicas);
    logger.info(
        "Dynamic num.stream.threads: totalTasks={} across {} sub-topologies, replicas={}, computed={}",
        totalTasks,
        subtopologyCount,
        replicas,
        threads);
    return OptionalInt.of(threads);
  }

  // Single-loop implementation: AdminClient.describeTopics() already fires all RPCs concurrently
  // before returning futures, so iteration here only consumes a shared deadline (now+timeout) —
  // total wall-clock is capped at DESCRIBE_TOPICS_TIMEOUT_MILLIS regardless of topic count.
  private Map<String, Integer> describePartitions(
      final AdminClient adminClient, final Set<String> topics) {
    if (topics.isEmpty()) {
      return Map.of();
    }
    final DescribeTopicsResult result = adminClient.describeTopics(topics);
    final Map<String, KafkaFuture<TopicDescription>> futures = result.topicNameValues();
    final long deadlineMillis = System.currentTimeMillis() + DESCRIBE_TOPICS_TIMEOUT_MILLIS;
    final Map<String, Integer> partitions = new HashMap<>();

    for (final Entry<String, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
      final long remainingMillis = deadlineMillis - System.currentTimeMillis();
      if (remainingMillis <= 0) {
        throw new RuntimeException(
            "Timed out describing topics after " + DESCRIBE_TOPICS_TIMEOUT_MILLIS + "ms");
      }
      try {
        partitions.put(
            entry.getKey(),
            entry.getValue().get(remainingMillis, TimeUnit.MILLISECONDS).partitions().size());
      } catch (final TimeoutException timeoutException) {
        throw new RuntimeException(
            "Timed out describing topic " + entry.getKey(), timeoutException);
      } catch (final InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while describing topic " + entry.getKey(), interruptedException);
      } catch (final ExecutionException executionException) {
        if (executionException.getCause() instanceof UnknownTopicOrPartitionException) {
          logger.warn(
              "Topic absent on broker: {}. Treating as 0 partitions; restart needed once created.",
              entry.getKey());
          partitions.put(entry.getKey(), 0);
        } else {
          throw new RuntimeException(
              "Failed to describe topic " + entry.getKey(), executionException);
        }
      }
    }
    return Map.copyOf(partitions);
  }
}
