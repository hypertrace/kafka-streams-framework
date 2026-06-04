package org.hypertrace.core.kafkastreams.framework.threading;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Function;
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
 * REPLICA_COUNT} environment variable injected by the deployment template. A non-positive value
 * (zero, negative, or unset) falls back to {@link #FALLBACK_NUM_STREAM_THREADS}.
 */
public class StreamThreadsCountResolver {

  public static final String DYNAMIC_SENTINEL = "DYNAMIC";

  // Reasonable default for current Hypertrace Kafka Streams apps. Apps that need a different
  // value should configure a numeric num.stream.threads explicitly instead of using DYNAMIC.
  public static final int FALLBACK_NUM_STREAM_THREADS = 8;

  private static final Logger logger = LoggerFactory.getLogger(StreamThreadsCountResolver.class);

  private final DynamicStreamThreadsCountCalculator calculator;
  private final IntSupplier replicaCountSupplier;
  private final Function<Properties, AdminClient> adminClientFactory;

  public StreamThreadsCountResolver(final IntSupplier replicaCountSupplier) {
    this(new DynamicStreamThreadsCountCalculator(), replicaCountSupplier, AdminClient::create);
  }

  public StreamThreadsCountResolver(
      final DynamicStreamThreadsCountCalculator calculator,
      final IntSupplier replicaCountSupplier,
      final Function<Properties, AdminClient> adminClientFactory) {
    this.calculator = calculator;
    this.replicaCountSupplier = replicaCountSupplier;
    this.adminClientFactory = adminClientFactory;
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
   * Resolve a thread count for the given topology. Returns {@link OptionalInt#empty()} when
   * prerequisites are missing (non-positive replica count), the topology is unsupported (regex
   * sources), or the AdminClient/calculator call fails — the caller substitutes its fallback so the
   * application can still start.
   */
  public OptionalInt resolve(final Topology topology, final Map<String, Object> streamsProperties) {
    final int replicas = replicaCountSupplier.getAsInt();
    if (replicas <= 0) {
      logger.warn("replica.count is non-positive ({}); skipping dynamic resolution", replicas);
      return OptionalInt.empty();
    }
    try (final AdminClient adminClient =
        adminClientFactory.apply(toProperties(streamsProperties))) {
      return calculator.compute(topology, adminClient, replicas);
    } catch (final RuntimeException runtimeException) {
      logger.error(
          "Failed to compute dynamic num.stream.threads; skipping dynamic resolution",
          runtimeException);
      return OptionalInt.empty();
    }
  }

  /**
   * Convenience adapter — given an {@code Optional<Integer>} replica-count source (the common
   * config-loaded shape), produce a supplier that yields the value when present and {@code 0} when
   * absent (treated as a fallback signal by {@link #resolve}).
   */
  public static IntSupplier optionalReplicaCount(final Optional<Integer> replicaCount) {
    return () -> replicaCount.orElse(0);
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
}
