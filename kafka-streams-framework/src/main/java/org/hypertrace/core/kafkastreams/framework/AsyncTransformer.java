package org.hypertrace.core.kafkastreams.framework;

import static org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp.KAFKA_STREAMS_CONFIG_KEY;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@SuppressWarnings("UnstableApiUsage")
public abstract class AsyncTransformer<K, V, KOUT, VOUT>
    implements Transformer<K, V, KeyValue<KOUT, VOUT>> {
  private final Executor executor;
  private BlockingQueue<CompletableFuture<List<KeyValue<KOUT, VOUT>>>> pendingFutures;
  private RateLimiter rateLimiter;
  private ProcessorContext context;
  private String transformerName;
  private static final String COMMIT_INTERVAL_CONFIG_KEY = "commitIntervalMs";
  private static final String MAX_BATCH_SIZE_CONFIG_KEY = "maxBatchSize";
  private static final String TRANSFORMERS_CONFIG_KEY = "async.transformers";

  // TODO: configurable executor - supplier pattern. This enables to use a common thread-pool for
  // all stream tasks
  public AsyncTransformer(int concurrency, int maxBatchSize, Duration flushInterval) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat("async-transformer-pool-%d")
            .setDaemon(true)
            .build();
    this.executor = Executors.newFixedThreadPool(concurrency, threadFactory);
    this.pendingFutures = new ArrayBlockingQueue<>(maxBatchSize);
    this.rateLimiter = RateLimiter.create(1.0 / flushInterval.toSeconds());
  }

  public AsyncTransformer(
      Supplier<Executor> executorSupplier, int maxBatchSize, Duration flushInterval) {
    this.executor = executorSupplier.get();
    this.pendingFutures = new ArrayBlockingQueue<>(maxBatchSize);
    this.rateLimiter = RateLimiter.create(1.0 / flushInterval.toSeconds());
  }

  public AsyncTransformer(Supplier<Executor> executorSupplier, String name) {
    this.executor = executorSupplier.get();
    this.transformerName = name;
  }

  @Override
  public final void init(ProcessorContext context) {
    this.context = context;
    Map<String, Object> appConfigs = context.appConfigs();
    Config kafkaStreamsConfig = (Config) appConfigs.get(KAFKA_STREAMS_CONFIG_KEY);
    Config transformerConfig =
        kafkaStreamsConfig.getConfig(TRANSFORMERS_CONFIG_KEY).getConfig(this.transformerName);
    Duration flushInterval =
        Duration.ofMillis(transformerConfig.getInt(COMMIT_INTERVAL_CONFIG_KEY));
    this.pendingFutures =
        new ArrayBlockingQueue<>(transformerConfig.getInt(MAX_BATCH_SIZE_CONFIG_KEY));
    this.rateLimiter = RateLimiter.create(1.0 / flushInterval.toSeconds());
    doInit(appConfigs);
  }

  protected abstract void doInit(Map<String, Object> appConfigs);

  public abstract List<KeyValue<KOUT, VOUT>> asyncTransform(K key, V value);

  @SneakyThrows
  @Override
  public final KeyValue<KOUT, VOUT> transform(K key, V value) {

    CompletableFuture<List<KeyValue<KOUT, VOUT>>> future =
        CompletableFuture.supplyAsync(() -> asyncTransform(key, value), executor);
    // with put, thread gets blocked when queue is full. queue consumer runs in this same thread.
    pendingFutures.put(future);

    // Flush based on size or time - whichever occurs first
    // once the queue is full, flush the queue.
    if (pendingFutures.remainingCapacity() == 0 || rateLimiter.tryAcquire()) {
      log.debug("flush start - queue size before flush: {}", pendingFutures.size());
      processResults();
      log.debug("flush end - queue size after flush: {}", pendingFutures.size());
    }
    return null;
  }

  @Override
  public void close() {
    // no-op
  }

  @SneakyThrows
  private void processResults() {
    while (!pendingFutures.isEmpty()) {
      CompletableFuture<List<KeyValue<KOUT, VOUT>>> future = pendingFutures.poll();
      // makes sure transformation is complete
      future.join();
      // another join is needed to make sure downstream forward is also complete
      future.thenAccept(result -> result.forEach(kv -> context.forward(kv.key, kv.value))).join();
    }
    // commit once per batch
    context.commit();
  }
}
