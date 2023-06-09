package org.hypertrace.core.kafkastreams.framework.async;

public class Constants {
  public static int DEFAULT_ASYNC_EXECUTOR_POOL_SIZE = 16;
  public static int DEFAULT_ASYNC_TRANSFORMER_BATCH_SIZE = 5120;
  public static int DEFAULT_ASYNC_TRANSFORMER_COMMIT_INTERVAL = 5000;
  public static String ASYNC_EXECUTOR_POOL_SIZE_KEY = "async.executors.maxPoolSize";
}
