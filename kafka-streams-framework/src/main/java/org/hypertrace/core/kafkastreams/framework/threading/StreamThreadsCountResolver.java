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
 * Resolves a concrete {@code num.stream.threads} value for an app that has opted into dynamic
 * thread sizing. Bridges the topology + AdminClient to the {@link
 * DynamicStreamThreadsCountCalculator} and returns {@link OptionalInt#empty()} when resolution
 * cannot produce a meaningful value — the caller keeps the configured numeric {@code
 * num.stream.threads}.
 *
 * <p>The replica count is supplied externally — applications typically wire it from a {@code
 * REPLICA_COUNT} environment variable injected by the deployment template. A non-positive value
 * (zero, negative, or unset) yields {@link OptionalInt#empty()}.
 */
public class StreamThreadsCountResolver {

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
   * Resolve a thread count for the given topology. Returns {@link OptionalInt#empty()} when
   * prerequisites are missing (non-positive replica count), the topology is unsupported (regex
   * sources), or the AdminClient/calculator call fails — the caller keeps the configured numeric
   * {@code num.stream.threads} so the application can still start with a sane value.
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
   * absent (treated as a "skip dynamic resolution" signal by {@link #resolve}).
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
