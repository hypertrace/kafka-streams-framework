package org.hypertrace.core.kafkastreams.framework.async;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Async version of the {@link Processor} interface. Offloads the entire processing to a different
 * executor(thread pool), collects the records to forward to the next stage of the topology and
 * flushes them after the processing is complete
 *
 * @param <K> the type of input keys
 * @param <V> the type of input values
 * @param <KOUT> the type of output keys
 * @param <VOUT> the type of output values
 */
@Slf4j
@SuppressWarnings("UnstableApiUsage")
public abstract class AsyncProcessor<K, V, KOUT, VOUT> implements Processor<K, V, KOUT, VOUT> {
  private List<Executor> executorList;
  private final BlockingQueue<CompletableFuture<List<RecordToForward<KOUT, VOUT>>>> pendingFutures;
  private final RateLimiter rateLimiter;
  private final Optional<KeyToAsyncThreadPartitioner<K>> mayBeKeyToAsyncThreadPartitioner;
  private ProcessorContext<KOUT, VOUT> context;

  public AsyncProcessor(
      Supplier<List<Executor>> executorListSupplier,
      AsyncProcessorConfig asyncProcessorConfig,
      Optional<KeyToAsyncThreadPartitioner<K>> mayBeKeyToAsyncThreadPartitioner) {
    this.executorList = executorListSupplier.get();
    this.mayBeKeyToAsyncThreadPartitioner = mayBeKeyToAsyncThreadPartitioner;
    if (executorList.size() == 1 && mayBeKeyToAsyncThreadPartitioner.isPresent()) {
      log.warn(
          "async processor has KeyToAsyncThreadPartitioner but with provided executor list no partitioning is respected!");
    }
    this.pendingFutures = new ArrayBlockingQueue<>(asyncProcessorConfig.getMaxBatchSize());
    this.rateLimiter =
        RateLimiter.create(1000.0 / asyncProcessorConfig.getCommitIntervalMs().toMillis());
    log.info(
        "async processor config. maxBatchSize: {}, commit rate: {}",
        this.pendingFutures.remainingCapacity(),
        this.rateLimiter.getRate());
    // warmup to prevent commit on first message
    rateLimiter.tryAcquire();
  }

  @Override
  public final void init(ProcessorContext<KOUT, VOUT> context) {
    this.context = context;
    doInit(context.appConfigs());
  }

  protected abstract void doInit(Map<String, Object> appConfigs);

  public abstract List<RecordToForward<KOUT, VOUT>> asyncProcess(K key, V value);

  @SneakyThrows
  @Override
  public void process(Record<K, V> record) {
    int numericalHashOfKey =
        mayBeKeyToAsyncThreadPartitioner
            .map(partitioner -> partitioner.getNumericalHashForKey(record.key()))
            .orElse(0);
    CompletableFuture<List<RecordToForward<KOUT, VOUT>>> future =
        CompletableFuture.supplyAsync(
            () -> asyncProcess(record.key(), record.value()),
            getExecutorForHash(numericalHashOfKey));
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
      CompletableFuture<List<RecordToForward<KOUT, VOUT>>> future = pendingFutures.poll();
      // makes sure processing is complete
      future.join();
      // another join is needed to make sure downstream forward is also complete
      future
          .thenAccept(
              result -> {
                if (result != null) {
                  result.forEach(
                      recordToForward ->
                          context.forward(
                              recordToForward.getRecord(), recordToForward.getChildName()));
                }
              })
          .join();
    }
    // commit once per batch
    context.commit();
  }

  private Executor getExecutorForHash(int numericalHashOfKey) {
    int partition = numericalHashOfKey % executorList.size();
    if (partition < 0) {
      partition += executorList.size();
    }
    return executorList.get(partition);
  }
}
