package org.hypertrace.core.kafkastreams.framework.async;

public class Constants {
  public static int DEFAULT_ASYNC_EXECUTOR_POOL_SIZE = 16;
  public static int DEFAULT_ASYNC_PROCESSOR_BATCH_SIZE = 5120;
  public static int DEFAULT_ASYNC_PROCESSOR_COMMIT_INTERVAL = 5000;
  public static String ASYNC_EXECUTOR_THREADS_COUNT_KEY = "async.executors.threadsCount";
}
