package org.hypertrace.core.kafkastreams.framework.threading;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;

class StreamThreadsCountResolverTest {

  @Test
  void calculatorThrowReturnsEmpty() {
    final AdminClient adminClient = mock(AdminClient.class);
    when(adminClient.describeTopics(anySet()))
        .thenThrow(new RuntimeException("simulated broker outage"));
    final StreamThreadsCountResolver resolver = resolverWith(() -> 8, adminClient);

    assertEquals(OptionalInt.empty(), resolver.resolve(simpleTopology(), bootstrapProperties()));
  }

  @Test
  void missingReplicaCountReturnsEmpty() {
    final IntSupplier missing = StreamThreadsCountResolver.optionalReplicaCount(Optional.empty());
    final StreamThreadsCountResolver resolver = resolverWith(missing, mock(AdminClient.class));

    assertEquals(OptionalInt.empty(), resolver.resolve(simpleTopology(), bootstrapProperties()));
  }

  @Test
  void zeroReplicaCountReturnsEmpty() {
    final StreamThreadsCountResolver resolver = resolverWith(() -> 0, mock(AdminClient.class));

    assertEquals(OptionalInt.empty(), resolver.resolve(simpleTopology(), bootstrapProperties()));
  }

  @Test
  void negativeReplicaCountReturnsEmpty() {
    final StreamThreadsCountResolver resolver = resolverWith(() -> -1, mock(AdminClient.class));

    assertEquals(OptionalInt.empty(), resolver.resolve(simpleTopology(), bootstrapProperties()));
  }

  @Test
  void delegatesToCalculatorWithConfiguredReplicas() {
    // 30-partition source / 8 replicas -> ceil(30/8) = 4 threads. Real calculator computes this;
    // only AdminClient (the external) is stubbed.
    final AdminClient adminClient = mock(AdminClient.class);
    stubPartitions(adminClient, Map.of("topic-a", 30));
    final StreamThreadsCountResolver resolver = resolverWith(() -> 8, adminClient);

    assertEquals(OptionalInt.of(4), resolver.resolve(simpleTopology(), bootstrapProperties()));
  }

  @Test
  void isDynamicTrueWhenSentinelSet() {
    assertEquals(
        true,
        StreamThreadsCountResolver.isDynamic(
            Map.of(
                StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                StreamThreadsCountResolver.DYNAMIC_SENTINEL)));
  }

  @Test
  void isDynamicFalseForNumericValueOrAbsent() {
    assertEquals(
        false,
        StreamThreadsCountResolver.isDynamic(Map.of(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4)));
    assertEquals(false, StreamThreadsCountResolver.isDynamic(Map.of()));
  }

  private static StreamThreadsCountResolver resolverWith(
      final IntSupplier replicaCountSupplier, final AdminClient adminClient) {
    final Function<Properties, AdminClient> factory = properties -> adminClient;
    return new StreamThreadsCountResolver(
        new DynamicStreamThreadsCountCalculator(), replicaCountSupplier, factory);
  }

  private static Topology simpleTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("topic-a", Consumed.with(Serdes.String(), Serdes.String()))
        .foreach((key, value) -> {});
    return builder.build();
  }

  private static Map<String, Object> bootstrapProperties() {
    return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  }

  private static void stubPartitions(
      final AdminClient adminClient, final Map<String, Integer> partitionsByTopic) {
    final DescribeTopicsResult result = mock(DescribeTopicsResult.class);
    final Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>();
    partitionsByTopic.forEach(
        (topic, partitionCount) -> futures.put(topic, futureFor(topic, partitionCount)));
    when(result.topicNameValues()).thenReturn(futures);
    when(adminClient.describeTopics(anySet())).thenReturn(result);
  }

  private static KafkaFuture<TopicDescription> futureFor(final String topic, final int partitions) {
    final List<TopicPartitionInfo> partitionInfos =
        IntStream.range(0, partitions)
            .mapToObj(index -> new TopicPartitionInfo(index, Node.noNode(), List.of(), List.of()))
            .collect(toUnmodifiableList());
    return KafkaFuture.completedFuture(new TopicDescription(topic, false, partitionInfos));
  }
}
