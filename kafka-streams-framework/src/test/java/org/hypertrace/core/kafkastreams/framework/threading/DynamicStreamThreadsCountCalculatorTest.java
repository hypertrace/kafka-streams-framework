package org.hypertrace.core.kafkastreams.framework.threading;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DynamicStreamThreadsCountCalculatorTest {

  private static final int ABSENT = -1;

  private final DynamicStreamThreadsCountCalculator calculator =
      new DynamicStreamThreadsCountCalculator();

  @Mock private AdminClient adminClient;

  @Test
  void singleSubtopologyWithThirtyPartitionsAndEightReplicasGivesFourThreads() {
    final Topology topology = topologyForSubtopologies(Set.of("topic-a"));
    stubPartitions(Map.of("topic-a", 30));

    assertEquals(OptionalInt.of(4), calculator.compute(topology, adminClient, 8));
  }

  @Test
  void twoSubtopologiesAreSummedThenDividedByReplicas() {
    final Topology topology = topologyForSubtopologies(Set.of("topic-a"), Set.of("topic-b"));
    stubPartitions(Map.of("topic-a", 10, "topic-b", 20));

    assertEquals(OptionalInt.of(4), calculator.compute(topology, adminClient, 8));
  }

  @Test
  void absentTopicCountsAsZeroPartitions() {
    final Topology topology = topologyForSubtopologies(Set.of("topic-a"), Set.of("topic-missing"));
    stubPartitions(Map.of("topic-a", 16, "topic-missing", ABSENT));

    assertEquals(OptionalInt.of(2), calculator.compute(topology, adminClient, 8));
  }

  @Test
  void zeroOrNegativeReplicasThrows() {
    final Topology topology = topologyForSubtopologies(Set.of("topic-a"));

    assertThrows(
        IllegalArgumentException.class, () -> calculator.compute(topology, adminClient, 0));
    assertThrows(
        IllegalArgumentException.class, () -> calculator.compute(topology, adminClient, -1));
  }

  // No resolvable partitions -> caller keeps the configured num.stream.threads as-is.
  @Test
  void totalTasksZeroReturnsEmpty() {
    final Topology topology = topologyForSubtopologies(Set.of("topic-a"));
    stubPartitions(Map.of("topic-a", ABSENT));

    assertEquals(OptionalInt.empty(), calculator.compute(topology, adminClient, 8));
  }

  // Regex/pattern subscriptions cannot be enumerated up-front against the broker, so dynamic
  // sizing would silently under-count tasks. Calculator must signal "not applicable" via empty
  // so the caller falls back to its configured default.
  @Test
  void patternSubscriptionReturnsEmpty() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(Pattern.compile("topic-.*"), Consumed.with(Serdes.String(), Serdes.String()))
        .foreach((key, value) -> {});
    final Topology topology = builder.build();

    assertEquals(OptionalInt.empty(), calculator.compute(topology, adminClient, 8));
  }

  @SafeVarargs
  private static Topology topologyForSubtopologies(final Set<String>... topicsBySubtopology) {
    final StreamsBuilder builder = new StreamsBuilder();
    for (final Set<String> topics : topicsBySubtopology) {
      for (final String topic : topics) {
        builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
            .foreach((key, value) -> {});
      }
    }
    return builder.build();
  }

  private void stubPartitions(final Map<String, Integer> partitionsByTopic) {
    final DescribeTopicsResult result = mock(DescribeTopicsResult.class);
    final Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>();
    partitionsByTopic.forEach(
        (topic, partitionCount) -> futures.put(topic, futureFor(topic, partitionCount)));
    when(result.topicNameValues()).thenReturn(futures);
    when(adminClient.describeTopics(anySet())).thenReturn(result);
  }

  private static KafkaFuture<TopicDescription> futureFor(final String topic, final int partitions) {
    if (partitions == ABSENT) {
      final KafkaFutureImpl<TopicDescription> failed = new KafkaFutureImpl<>();
      failed.completeExceptionally(new UnknownTopicOrPartitionException(topic));
      return failed;
    }
    final List<TopicPartitionInfo> partitionInfos =
        IntStream.range(0, partitions)
            .mapToObj(index -> new TopicPartitionInfo(index, Node.noNode(), List.of(), List.of()))
            .collect(toUnmodifiableList());
    return KafkaFuture.completedFuture(new TopicDescription(topic, false, partitionInfos));
  }
}
