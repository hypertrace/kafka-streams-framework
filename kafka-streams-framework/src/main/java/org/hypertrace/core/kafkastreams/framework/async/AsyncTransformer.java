package org.hypertrace.core.kafkastreams.framework.async;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
  private final BlockingQueue<CompletableFuture<List<KeyValue<KOUT, VOUT>>>> pendingFutures;
  private final RateLimiter rateLimiter;
  private ProcessorContext context;

  public AsyncTransformer(
      Supplier<Executor> executorSupplier, AsyncTransformerConfig asyncTransformerConfig) {
    this.executor = executorSupplier.get();
    this.pendingFutures = new ArrayBlockingQueue<>(asyncTransformerConfig.getMaxBatchSize());
    this.rateLimiter =
        RateLimiter.create(1.0 / asyncTransformerConfig.getCommitIntervalMs().toSeconds());
  }

  @Override
  public final void init(ProcessorContext context) {
    this.context = context;
    doInit(context.appConfigs());
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