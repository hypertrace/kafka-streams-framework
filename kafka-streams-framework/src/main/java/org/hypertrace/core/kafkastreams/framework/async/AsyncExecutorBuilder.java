package org.hypertrace.core.kafkastreams.framework.async;

import static org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp.KAFKA_STREAMS_CONFIG_KEY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class AsyncExecutorBuilder {
  private final Config jobConfig;

  public AsyncExecutorBuilder(Config jobConfig) {
    this.jobConfig = jobConfig;
  }

  public Executor buildExecutor() {
    int concurrency =
        jobConfig.getConfig(KAFKA_STREAMS_CONFIG_KEY).getInt("async.executors.maxPoolSize");
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("kafka-streams-async-worker-%d")
            .setDaemon(true)
            .build();
    return Executors.newFixedThreadPool(concurrency, threadFactory);
  }
}
