package org.hypertrace.core.kafkastreams.framework;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
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
  private final BlockingQueue<CompletableFuture<List<KeyValue<KOUT, VOUT>>>> pendingFutures;
  private final RateLimiter rateLimiter;
  private ProcessorContext context;

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

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  public abstract List<KeyValue<KOUT, VOUT>> asyncTransform(K key, V value);

  @SneakyThrows
  @Override
  public KeyValue<KOUT, VOUT> transform(K key, V value) {
    // Flush based on time duration
    if (rateLimiter.tryAcquire()) {
      log.info("flush start - type: time, queue size: {}", pendingFutures.size());
      processResults();
      log.info("flush end - type: time, queue size: {}", pendingFutures.size());
    }

    CompletableFuture<List<KeyValue<KOUT, VOUT>>> future =
        CompletableFuture.supplyAsync(() -> asyncTransform(key, value), executor);
    // thread blocked when queue is full. queue consumer runs in this same thread.
    // once the queue is full, flush the queue
    if (!pendingFutures.offer(future)) {
      log.info("flush start - type: size. queue size: {}", pendingFutures.size());
      processResults();
      log.info("flush end - type: size. queue size: {}", pendingFutures.size());
      pendingFutures.put(future);
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
      future.join();
      future
          .thenAccept((result) -> result.forEach((kv) -> context.forward(kv.key, kv.value)))
          .exceptionally(this::logAndRethrow);
      context.commit();
    }
  }

  @SneakyThrows
  private Void logAndRethrow(Throwable e) {
    throw e;
  }
}
