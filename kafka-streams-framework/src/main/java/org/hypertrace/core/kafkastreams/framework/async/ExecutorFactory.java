package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.async.Constants.ASYNC_EXECUTOR_THREADS_COUNT_KEY;
import static org.hypertrace.core.kafkastreams.framework.async.Constants.DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class ExecutorFactory {
  private static List<Executor> executorList;

  /** usePool should be set to false if you use KeyToAsyncThreadPartitioner */
  public static synchronized Supplier<List<Executor>> getExecutorListSupplier(
      Config config, boolean usePool) {
    if (executorList == null) {
      int threadsCount =
          config.hasPath(ASYNC_EXECUTOR_THREADS_COUNT_KEY)
              ? config.getInt(ASYNC_EXECUTOR_THREADS_COUNT_KEY)
              : DEFAULT_ASYNC_EXECUTOR_POOL_SIZE;

      if (threadsCount == 0) {
        // direct/sync execution when pool size is explicitly configured to a value <= 0
        executorList = List.of(Runnable::run);
      } else if (usePool) {
        ThreadFactory threadFactory =
            new ThreadFactoryBuilder()
                .setNameFormat("kafka-streams-async-worker-%d")
                .setDaemon(true)
                .build();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadsCount, threadFactory);
        PlatformMetricsRegistry.monitorExecutorService(
            "kafka-streams-async-pool", fixedThreadPool, null);
        executorList = List.of(fixedThreadPool);
      } else {
        executorList = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++) {
          ThreadFactory threadFactory =
              new ThreadFactoryBuilder()
                  .setNameFormat("kafka-streams-async-worker-" + i)
                  .setDaemon(true)
                  .build();
          ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(threadFactory);
          PlatformMetricsRegistry.monitorExecutorService(
              "kafka-streams-async-thread-" + i, singleThreadExecutor, null);
          executorList.add(singleThreadExecutor);
        }
      }
    }
    return () -> executorList;
  }
}
