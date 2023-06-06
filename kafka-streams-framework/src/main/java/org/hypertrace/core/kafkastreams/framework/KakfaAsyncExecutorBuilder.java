package org.hypertrace.core.kafkastreams.framework;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KakfaAsyncExecutorBuilder {

  public Executor buildExecutor(int concurrency) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("async-transformer-pool-%d")
            .setDaemon(true)
            .build();
    return Executors.newFixedThreadPool(concurrency, threadFactory);
  }
}
