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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

@Slf4j
public abstract class AsyncLegacyProcessor<K, V> implements Processor<K, V> {

  private final Executor executor;
  private final BlockingQueue<CompletableFuture<List<Pair<KeyValue, To>>>> pendingFutures;
  private final RateLimiter rateLimiter;
  private ProcessorContext context;

  public AsyncLegacyProcessor(
      Supplier<Executor> executorSupplier, AsyncTransformerConfig asyncTransformerConfig) {
    this.executor = executorSupplier.get();
    this.pendingFutures = new ArrayBlockingQueue<>(asyncTransformerConfig.getMaxBatchSize());
    this.rateLimiter =
        RateLimiter.create(1000.0 / asyncTransformerConfig.getCommitIntervalMs().toMillis());
    log.info(
        "async transformer config. maxBatchSize: {}, commit rate: {}",
        this.pendingFutures.remainingCapacity(),
        this.rateLimiter.getRate());
    // warmup to prevent commit on first message
    rateLimiter.tryAcquire();
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    doInit(context.appConfigs());
  }

  protected abstract void doInit(Map<String, Object> appConfigs);

  public abstract List<Pair<KeyValue, To>> asyncProcess(K key, V value);

  @SneakyThrows
  @Override
  public void process(K key, V value) {
    CompletableFuture<List<Pair<KeyValue, To>>> future =
        CompletableFuture.supplyAsync(() -> asyncProcess(key, value), executor);
    // with put, thread gets blocked when queue is full. queue consumer runs in this same thread.
    pendingFutures.put(future);

    // Flush based on size or time - whichever occurs first
    // once the queue is full, flush the queue.
    if (pendingFutures.remainingCapacity() == 0 || rateLimiter.tryAcquire()) {
      log.debug("flush start - queue size before flush: {}", pendingFutures.size());
      processResults();
      log.debug("flush end - queue size after flush: {}", pendingFutures.size());
    }
  }

  @SneakyThrows
  private void processResults() {
    while (!pendingFutures.isEmpty()) {
      CompletableFuture<List<Pair<KeyValue, To>>> future = pendingFutures.poll();
      // makes sure transformation is complete
      future.join();
      // another join is needed to make sure downstream forward is also complete
      future
          .thenAccept(
              result -> {
                if (result != null) {
                  result.forEach(
                      pair -> {
                        if (pair.getValue() == null) {
                          context.forward(pair.getKey().key, pair.getKey().value);
                        }
                        context.forward(pair.getKey().key, pair.getKey().value, pair.getValue());
                      });
                }
              })
          .join();
    }
    // commit once per batch
    context.commit();
  }

  @Override
  public void close() {
    // no-op
  }
}
