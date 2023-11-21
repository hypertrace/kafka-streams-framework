package org.hypertrace.core.kafka.event.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaEventListenerTest {

  @Test
  void testThrowOnInvalidInputs() {
    // no callback
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new KafkaEventListener.Builder<String, Long>()
                .build(
                    "",
                    ConfigFactory.parseMap(Map.of("topic.name", "")),
                    new MockConsumer<>(OffsetResetStrategy.LATEST)));
    // no topic name
    assertThrows(
        ConfigException.class,
        () ->
            new KafkaEventListener.Builder<String, Long>()
                .registerCallback((String k, Long v) -> System.out.println(k + ":" + v))
                .build("", ConfigFactory.empty(), new MockConsumer<>(OffsetResetStrategy.LATEST)));
  }

  @Test
  void testEventModificationCache() throws Exception {
    // kafka consumer mock setup
    MockConsumer<String, Long> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    String topic = "event-update-topic";
    kafkaConsumer.updatePartitions(
        topic,
        List.of(
            getPartitionInfo(topic, 0),
            getPartitionInfo(topic, 1),
            getPartitionInfo(topic, 2),
            getPartitionInfo(topic, 3)));
    HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(new TopicPartition(topic, 0), 50L);
    endOffsets.put(new TopicPartition(topic, 1), 50L);
    endOffsets.put(new TopicPartition(topic, 2), 50L);
    endOffsets.put(new TopicPartition(topic, 3), 50L);
    kafkaConsumer.updateEndOffsets(endOffsets);
    // create instance of event modification cache consuming from this consumer
    EventModificationCache eventModificationCache =
        new EventModificationCache(
            "modification-event-consumer",
            ConfigFactory.parseMap(Map.of("topic.name", topic, "poll.timeout", "5ms")),
            kafkaConsumer);
    Thread.sleep(10);
    assertEquals(10L, eventModificationCache.get(10));
    assertEquals(100L, eventModificationCache.get(100));
    // not present key won't trigger any population but callback function should be called
    kafkaConsumer.addRecord(new ConsumerRecord<>(topic, 0, 100, "32", 89L));
    Thread.sleep(100);
    assertFalse(eventModificationCache.hasKey(32));
    // existing key will be modified based on entry
    kafkaConsumer.addRecord(new ConsumerRecord<>(topic, 3, 200, "10", -3L));
    Thread.sleep(100);
    assertEquals(-3L, eventModificationCache.get(10));
    eventModificationCache.close();
  }

  private PartitionInfo getPartitionInfo(String topic, int partition) {
    return new PartitionInfo(topic, partition, mock(Node.class), new Node[0], new Node[0]);
  }

  static class EventModificationCache {
    private final AsyncLoadingCache<Integer, Long> cache;
    private final KafkaEventListener<String, Long> eventListener;

    EventModificationCache(
        String consumerName, Config kafkaConfig, Consumer<String, Long> consumer) {
      cache =
          Caffeine.newBuilder()
              .maximumSize(10_000)
              .expireAfterAccess(Duration.ofHours(6))
              .refreshAfterWrite(Duration.ofHours(1))
              .buildAsync(this::load);
      eventListener =
          new KafkaEventListener.Builder<String, Long>()
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
