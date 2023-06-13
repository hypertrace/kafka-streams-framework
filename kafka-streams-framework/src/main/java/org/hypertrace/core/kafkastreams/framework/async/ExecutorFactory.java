package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.ASYNC_EXECUTOR_POOL_SIZE_KEY;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

public class ExecutorFactory {
  private static Executor executor;

  public static synchronized Supplier<Executor> getExecutorSupplier(Config config) {
    if (executor == null) {
      int poolSize =
          config.hasPath(ASYNC_EXECUTOR_POOL_SIZE_KEY)
              ? config.getInt(ASYNC_EXECUTOR_POOL_SIZE_KEY)
              : DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;

      ThreadFactory threadFactory =
          new ThreadFactoryBuilder()
              .setNameFormat("kafka-streams-async-worker-%d")
              .setDaemon(true)
              .build();
      executor = Executors.newFixedThreadPool(poolSize, threadFactory);
    }
    return () -> executor;
  }
}
