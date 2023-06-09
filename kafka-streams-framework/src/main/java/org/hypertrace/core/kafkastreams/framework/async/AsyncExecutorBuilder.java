package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.ASYNC_EXECUTOR_POOL_SIZE_KEY;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class AsyncExecutorBuilder {
  private final int poolSize;

  private Executor executor;

  AsyncExecutorBuilder(int poolSize) {
    this.poolSize = poolSize;
  }

  public static AsyncExecutorBuilder withConfig(Config config) {
    int poolSize =
        config.hasPath(ASYNC_EXECUTOR_POOL_SIZE_KEY)
            ? config.getInt(ASYNC_EXECUTOR_POOL_SIZE_KEY)
            : DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;
    return new AsyncExecutorBuilder(poolSize);
  }

  public synchronized Executor build() {
    if (this.executor != null) {
      return this.executor;
    }
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("kafka-streams-async-worker-%d")
            .setDaemon(true)
            .build();
    executor = Executors.newFixedThreadPool(this.poolSize, threadFactory);
    return executor;
  }
}
