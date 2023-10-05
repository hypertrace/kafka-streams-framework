package org.hypertrace.core.kafkastreams.framework.async;

public class Constants {
  public static int DEFAULT_ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR = 4;
  public static int DEFAULT_ASYNC_PROCESSOR_BATCH_SIZE = 10240;
  public static int DEFAULT_ASYNC_PROCESSOR_COMMIT_INTERVAL = 30000;
  public static String ASYNC_EXECUTOR_POOL_MULTIPLICATION_FACTOR_KEY =
      "async.executors.pool.multiplication.factor";
}
