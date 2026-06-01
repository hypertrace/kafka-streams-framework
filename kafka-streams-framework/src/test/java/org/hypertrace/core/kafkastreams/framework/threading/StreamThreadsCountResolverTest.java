package org.hypertrace.core.kafkastreams.framework.threading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.function.IntSupplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamThreadsCountResolverTest {

  @Mock private DynamicStreamThreadsCountCalculator calculator;

  @Test
  void calculatorThrowFallsBackToEight() {
    final Topology topology = simpleTopology();
    when(calculator.compute(eq(topology), any(), eq(8)))
        .thenThrow(new RuntimeException("simulated broker outage"));
    final StreamThreadsCountResolver resolver = new StreamThreadsCountResolver(calculator, () -> 8);

    final int resolved = resolver.resolve(topology, bootstrapProperties());

    assertEquals(StreamThreadsCountResolver.FALLBACK_NUM_STREAM_THREADS, resolved);
  }

  @Test
  void missingReplicaCountFallsBack() {
    final IntSupplier missing = StreamThreadsCountResolver.optionalReplicaCount(Optional.empty());
    final StreamThreadsCountResolver resolver = new StreamThreadsCountResolver(calculator, missing);

    final int resolved = resolver.resolve(simpleTopology(), bootstrapProperties());

    assertEquals(StreamThreadsCountResolver.FALLBACK_NUM_STREAM_THREADS, resolved);
  }

  @Test
  void zeroReplicaCountFallsBack() {
    final StreamThreadsCountResolver resolver = new StreamThreadsCountResolver(calculator, () -> 0);

    final int resolved = resolver.resolve(simpleTopology(), bootstrapProperties());

    assertEquals(StreamThreadsCountResolver.FALLBACK_NUM_STREAM_THREADS, resolved);
  }

  @Test
  void negativeReplicaCountFallsBack() {
    final StreamThreadsCountResolver resolver =
        new StreamThreadsCountResolver(calculator, () -> -1);

    final int resolved = resolver.resolve(simpleTopology(), bootstrapProperties());

    assertEquals(StreamThreadsCountResolver.FALLBACK_NUM_STREAM_THREADS, resolved);
  }

  @Test
  void delegatesToCalculatorWithConfiguredReplicas() {
    final Topology topology = simpleTopology();
    when(calculator.compute(eq(topology), any(), eq(8))).thenReturn(7);
    final StreamThreadsCountResolver resolver = new StreamThreadsCountResolver(calculator, () -> 8);

    final int resolved = resolver.resolve(topology, bootstrapProperties());

    assertEquals(7, resolved);
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

  private static Topology simpleTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("topic-a", Consumed.with(Serdes.String(), Serdes.String()))
        .foreach((key, value) -> {});
    return builder.build();
  }

  private static Map<String, Object> bootstrapProperties() {
    return Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  }
}
