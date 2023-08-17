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

/**
 * Async version of the {@link Transformer} interface. Offloads the entire processing to a different
 * executor(thread pool), collects the records to forward to the next stage of the topology and
 * flushes them after the processing is complete
 *
 * @param <K> the type of input keys
 * @param <V> the type of input values
 * @param <KOUT> the type of output keys
 * @param <VOUT> the type of output values
 * @deprecated Use {@link AsyncProcessor} instead
 */
@Slf4j
@Deprecated
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
        RateLimiter.create(1000.0 / asyncTransformerConfig.getCommitIntervalMs().toMillis());
    log.info(
        "async transformer config. maxBatchSize: {}, commit rate: {}",
        this.pendingFutures.remainingCapacity(),
        this.rateLimiter.getRate());
    // warmup to prevent commit on first message
    rateLimiter.tryAcquire();
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
      future
          .thenAccept(
              result -> {
                if (result != null) {
                  result.forEach(kv -> context.forward(kv.key, kv.value));
                }
              })
          .join();
    }
    // commit once per batch
    context.commit();
  }
}
