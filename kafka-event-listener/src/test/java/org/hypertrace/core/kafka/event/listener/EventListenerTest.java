package org.hypertrace.core.kafka.event.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class EventListenerTest {

  @Test
  void testEventModificationCache() throws Exception {
    EventModificationCache eventModificationCache = new EventModificationCache();
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
    // register consumer
    eventModificationCache.registerConsumer(
        new KafkaEventListenerConsumer<>(
            "modification-event-consumer",
            ConfigFactory.parseMap(Map.of("topic.name", topic, "poll.timeout", "9ms")),
            kafkaConsumer,
            eventModificationCache::actOnEvent));
    Thread.sleep(10);
    assertEquals(10L, eventModificationCache.get(10));
    assertEquals(100L, eventModificationCache.get(100));
    // not present key won't trigger any population
    kafkaConsumer.addRecord(new ConsumerRecord<>(topic, 0, 100, "32", 89L));
    Thread.sleep(10);
    assertFalse(eventModificationCache.hasKey(32));
    // existing key will be modified based on entry
    kafkaConsumer.addRecord(new ConsumerRecord<>(topic, 3, 200, "10", -3L));
    Thread.sleep(10);
    assertEquals(-3L, eventModificationCache.get(10));
    eventModificationCache.close();
  }

  private PartitionInfo getPartitionInfo(String topic, int partition) {
    return new PartitionInfo(topic, partition, mock(Node.class), new Node[0], new Node[0]);
  }

  static class EventModificationCache extends EventListener {
    final AsyncLoadingCache<Integer, Long> cache;

    EventModificationCache() {
      cache =
          Caffeine.newBuilder()
              .maximumSize(10_000)
              .expireAfterAccess(Duration.ofHours(6))
              .refreshAfterWrite(Duration.ofHours(1))
              .buildAsync(this::load);
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

    @Override
    public <D, C> void actOnEvent(D eventKey, C eventValue) {
      if (eventKey.getClass().equals(String.class) && eventValue.getClass().equals(Long.class)) {
        int key = Integer.parseInt((String) eventKey);
        if (cache.asMap().containsKey(key)) {
          long value = (Long) eventValue;
          cache.put(key, CompletableFuture.completedFuture(value));
        }
      }
    }
  }
}
