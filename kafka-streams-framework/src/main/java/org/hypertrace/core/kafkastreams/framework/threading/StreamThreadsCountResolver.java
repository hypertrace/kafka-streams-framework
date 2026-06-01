package org.hypertrace.core.kafkastreams.framework.threading;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.IntSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a concrete {@code num.stream.threads} value when the application has configured the
 * {@link #DYNAMIC_SENTINEL} sentinel. Bridges the topology + AdminClient to the {@link
 * DynamicStreamThreadsCountCalculator} and falls back to {@link #FALLBACK_NUM_STREAM_THREADS} when
 * resolution fails.
 *
 * <p>The replica count is supplied externally — applications typically wire it from a {@code
 * REPLICA_COUNT} environment variable injected by the deployment template.
 */
public class StreamThreadsCountResolver {

  public static final String DYNAMIC_SENTINEL = "DYNAMIC";
  static final int FALLBACK_NUM_STREAM_THREADS = 8;

  private static final Logger logger = LoggerFactory.getLogger(StreamThreadsCountResolver.class);

  private final DynamicStreamThreadsCountCalculator calculator;
  private final IntSupplier replicaCountSupplier;

  public StreamThreadsCountResolver(final IntSupplier replicaCountSupplier) {
    this(new DynamicStreamThreadsCountCalculator(), replicaCountSupplier);
  }

  StreamThreadsCountResolver(
      final DynamicStreamThreadsCountCalculator calculator,
      final IntSupplier replicaCountSupplier) {
    this.calculator = calculator;
    this.replicaCountSupplier = replicaCountSupplier;
  }

  /**
   * Returns true if the {@code num.stream.threads} value in the given streams properties is the
   * dynamic sentinel and should be resolved by this class.
   */
  public static boolean isDynamic(final Map<String, Object> streamsProperties) {
    final Object value =
        streamsProperties.get(org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG);
    return value != null && DYNAMIC_SENTINEL.equals(String.valueOf(value));
  }

  /**
   * Resolve a concrete thread count for the given topology. Returns {@link
   * #FALLBACK_NUM_STREAM_THREADS} on any failure so the application can still start.
   */
  public int resolve(final Topology topology, final Map<String, Object> streamsProperties) {
    try {
      final int replicas = requirePositiveReplicaCount();
      try (final AdminClient adminClient = AdminClient.create(toProperties(streamsProperties))) {
        return calculator.compute(topology, adminClient, replicas);
      }
    } catch (final RuntimeException runtimeException) {
      logger.error(
          "Failed to compute dynamic num.stream.threads; falling back to {}",
          FALLBACK_NUM_STREAM_THREADS,
          runtimeException);
      return FALLBACK_NUM_STREAM_THREADS;
    }
  }

  /**
   * Convenience adapter — given an {@code Optional<Integer>} replica-count source (the common
   * config-loaded shape), produce a supplier that this resolver accepts.
   */
  public static IntSupplier optionalReplicaCount(final Optional<Integer> replicaCount) {
    return () ->
        replicaCount.orElseThrow(
            () -> new IllegalStateException("replica.count is not configured"));
  }

  private static Properties toProperties(final Map<String, Object> streamsProperties) {
    final Properties properties = new Properties();
    streamsProperties.forEach(
        (key, value) -> {
          if (value != null) {
            properties.put(key, value);
          }
        });
    return properties;
  }

  private int requirePositiveReplicaCount() {
    final int replicas = replicaCountSupplier.getAsInt();
    if (replicas <= 0) {
      throw new IllegalStateException("replica.count must be positive, got " + replicas);
    }
    return replicas;
  }
}
