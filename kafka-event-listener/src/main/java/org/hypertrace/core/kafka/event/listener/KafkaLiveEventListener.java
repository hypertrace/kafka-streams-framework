package org.hypertrace.core.kafka.event.listener;

import com.typesafe.config.Config;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * KafkaLiveEventListener consumes events produced to a single Kafka Topic from its initialisation
 * and on every message invokes provided callbacks. The thread safety of callback method must be
 * ensured by provider. It is important to note that there is no guarantee that all messages on
 * topic are consumed by this listener as every time we create a listener we consume from latest
 * offsets by design.
 *
 * <p>Operational Caveat: The listener subscribes to active partitions only, in case the partition
 * count changes once the listener is active, it will be opaque to those new partitions and will not
 * consume them. Only a new listener will go and fetch all active partitions hence it will be
 * required to restart the consumer application.
 *
 * <p>Typical usage of this listener is to back the remote caches to have lower latency of refresh
 * by generating respective information on kafka topics.
 *
 * <p>Refer to
 * org.hypertrace.core.kafka.event.listener.KafkaLiveEventListenerTest#testEventModificationCache()
 * for sample usage and test. Note that testing requires Thread.sleep > poll timeout in between
 */
public class KafkaLiveEventListener<K, V> implements AutoCloseable {
  private final KafkaLiveEventListenerCallable<K, V> kafkaLiveEventListenerCallable;
  private final Future<Void> kafkaLiveEventListenerCallableFuture;
  private final ExecutorService executorService;
  private final boolean cleanupExecutor;

  private KafkaLiveEventListener(
      KafkaLiveEventListenerCallable<K, V> kafkaLiveEventListenerCallable,
      ExecutorService executorService,
      boolean cleanupExecutor) {
    this.executorService = executorService;
    this.cleanupExecutor = cleanupExecutor;
    this.kafkaLiveEventListenerCallable = kafkaLiveEventListenerCallable;
    this.kafkaLiveEventListenerCallableFuture =
        executorService.submit(kafkaLiveEventListenerCallable);
  }

  public KafkaLiveEventListener<K, V> registerCallback(
      BiConsumer<? super K, ? super V> callbackFunction) {
    kafkaLiveEventListenerCallable.addCallback(callbackFunction);
    return this;
  }

  @Override
  public void close() throws Exception {
    kafkaLiveEventListenerCallableFuture.cancel(true);
    if (cleanupExecutor) {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  public static final class Builder<K, V> {
    private final Collection<BiConsumer<? super K, ? super V>> callbacks =
        new ConcurrentLinkedQueue<>();
    private ExecutorService executorService;
    private boolean cleanupExecutor =
        true; // if builder creates executor shutdown executor while closing event listener

    /** use registerCallback on the built instance instead */
    @Deprecated(forRemoval = true)
    public Builder<K, V> registerCallback(BiConsumer<? super K, ? super V> callbackFunction) {
      callbacks.add(callbackFunction);
      return this;
    }

    public Builder<K, V> withExecutorService(
        ExecutorService executorService, boolean cleanupExecutor) {
      this.executorService = executorService;
      this.cleanupExecutor = cleanupExecutor;
      return this;
    }

    public KafkaLiveEventListener<K, V> build(
        String consumerName, Config kafkaConfig, Consumer<K, V> kafkaConsumer) {
      if (executorService == null) {
        executorService =
            Executors.newSingleThreadExecutor(new ListenerThreadFactory(consumerName));
      }
      return new KafkaLiveEventListener<>(
          new KafkaLiveEventListenerCallable<>(consumerName, kafkaConfig, kafkaConsumer, callbacks),
          executorService,
          cleanupExecutor);
    }
  }
}

class ListenerThreadFactory implements ThreadFactory {
  private final String name;

  public ListenerThreadFactory(String consumerName) {
    this.name = "kafka-live-event-listener-" + consumerName;
  }

  public Thread newThread(Runnable r) {
    return new Thread(r, name);
  }
}
