package org.hypertrace.core.kafka.event.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

class KafkaLiveEventListenerTest {

  @Test
  void testThrowOnInvalidInputs() {
    // no callback
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new KafkaLiveEventListener.Builder<String, Long>()
                .build(
                    "",
                    ConfigFactory.parseMap(Map.of("topic.name", "")),
                    new MockConsumer<>(OffsetResetStrategy.LATEST)));
    // no topic name
    assertThrows(
        ConfigException.class,
        () ->
            new KafkaLiveEventListener.Builder<String, Long>()
                .registerCallback((String k, Long v) -> System.out.println(k + ":" + v))
                .build("", ConfigFactory.empty(), new MockConsumer<>(OffsetResetStrategy.LATEST)));
  }

  @Test
  void testEventModificationCache() throws Exception {
    // kafka consumer mock setup
    String topicName = "event-update-topic";
    KafkaMockConsumerTestUtil<String, Long> mockConsumerTestUtil =
        new KafkaMockConsumerTestUtil<>(topicName, 4);
    // create instance of event modification cache consuming from this consumer
    EventModificationCache eventModificationCache =
        new EventModificationCache(
            "modification-event-consumer",
            ConfigFactory.parseMap(Map.of("topic.name", topicName, "poll.timeout", "5ms")),
            mockConsumerTestUtil.getMockConsumer());
    Thread.sleep(10);
    assertEquals(10L, eventModificationCache.get(10));
    assertEquals(100L, eventModificationCache.get(100));
    // not present key won't trigger any population but callback function should be called
    mockConsumerTestUtil.addRecordToPartition(0, "32", 89L);
    Thread.sleep(100);
    assertFalse(eventModificationCache.hasKey(32));
    // existing key will be modified based on entry
    mockConsumerTestUtil.addRecordToPartition(3, "10", -3L);
    Thread.sleep(100);
    assertEquals(-3L, eventModificationCache.get(10));
    eventModificationCache.close();
  }

  static class EventModificationCache {
    private final AsyncLoadingCache<Integer, Long> cache;
    private final KafkaLiveEventListener<String, Long> eventListener;

    EventModificationCache(
        String consumerName, Config kafkaConfig, Consumer<String, Long> consumer) {
      cache =
          Caffeine.newBuilder()
              .maximumSize(10_000)
              .expireAfterAccess(Duration.ofHours(6))
              .refreshAfterWrite(Duration.ofHours(1))
              .buildAsync(this::load);
      eventListener =
          new KafkaLiveEventListener.Builder<String, Long>()
              .registerCallback(this::actOnEvent)
              .registerCallback(this::log)
              .build(consumerName, kafkaConfig, consumer);
    }

    public void close() throws Exception {
      eventListener.close();
    }

    long get(int key) throws Exception {
      return cache.get(key).get(10, TimeUnit.SECONDS);
    }

    boolean hasKey(int key) {
      return cache.asMap().containsKey(key);
    }

    private Long load(Integer key) {
      // ideally this will be remote call, just for sake of dummy test we returned a cast
      return (long) (key);
    }

    public void actOnEvent(String eventKey, Long eventValue) {
      int key = Integer.parseInt(eventKey);
      if (cache.asMap().containsKey(key)) {
        long value = eventValue;
        cache.put(key, CompletableFuture.completedFuture(value));
      }
    }

    // just a dummy logger to showcase multiple callbacks
    public void log(String eventKey, Long eventValue) {
      System.out.println("updated cache with event data from topic " + eventKey + ":" + eventValue);
    }
  }
}
