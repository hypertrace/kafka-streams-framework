package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR_KEY;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import org.apache.kafka.streams.StreamsConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class ExecutorFactory {
  private static Executor executor;

  /** Config input should be complete kafka streams config */
  public static synchronized Supplier<Executor> getExecutorSupplier(Config config) {
    if (executor == null) {
      int poolSize =
          config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG)
              * (config.hasPath(ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR_KEY)
                  ? config.getInt(ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR_KEY)
                  : DEFAULT_ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR);

      if (poolSize > 0) {
        ThreadFactory threadFactory =
            new ThreadFactoryBuilder()
                .setNameFormat("kafka-streams-async-worker-%d")
                .setDaemon(true)
                .build();
        ExecutorService executorSvc = Executors.newFixedThreadPool(poolSize, threadFactory);
        PlatformMetricsRegistry.monitorExecutorService(
            "kafka-streams-async-pool", executorSvc, null);
        executor = executorSvc;
      } else {
        // direct/sync execution when pool size is explicitly configured to a value <= 0
        executor = Runnable::run;
      }
    }
    return () -> executor;
  }
}
