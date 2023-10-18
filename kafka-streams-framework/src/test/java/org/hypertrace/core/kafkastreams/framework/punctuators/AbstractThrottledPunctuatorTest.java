package org.hypertrace.core.kafkastreams.framework.punctuators;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.CompletedTaskResult;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.RescheduleTaskResult;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.TaskResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractThrottledPunctuatorTest {
  TestPunctuator underTest;
  Clock clock;
  KeyValueStore<Long, List<String>> objectStore;

  @BeforeEach
  void setup() {
    clock = spy(Clock.systemUTC());
    MockProcessorContext<String, String> context = new MockProcessorContext<>();
    KeyValueStoreBuilder storeBuilder =
        new KeyValueStoreBuilder<>(
            Stores.inMemoryKeyValueStore("testStore"),
            Serdes.Long(),
            Serdes.ListSerde(ArrayList.class, Serdes.String()),
            Time.SYSTEM);
    storeBuilder.withCachingDisabled().withLoggingDisabled();
    objectStore = spy(storeBuilder.build());
    objectStore.init(context.getStateStoreContext(), objectStore);
    underTest =
        new TestPunctuator(
            clock,
            new ThrottledPunctuatorConfig(
                ConfigFactory.parseMap(
                    Map.of("testPunctuator.yield.ms", 100, "testPunctuator.window.ms", 5)),
                "testPunctuator"),
            objectStore);
  }

  @Test
  void testScheduleTask() {
    underTest.scheduleTask(100, "schedule1");
    assertEquals(List.of("schedule1"), objectStore.get(100L));
    underTest.scheduleTask(103, "schedule2");
    assertEquals(List.of("schedule1", "schedule2"), objectStore.get(100L));
    underTest.scheduleTask(201, "schedule3");
    assertEquals(List.of("schedule1", "schedule2"), objectStore.get(100L));
    assertEquals(List.of("schedule3"), objectStore.get(200L));
  }

  @Test
  void testRescheduleTask() {
    underTest.scheduleTask(100, "schedule1");
    underTest.scheduleTask(103, "schedule2");
    assertEquals(List.of("schedule1", "schedule2"), objectStore.get(100L));
    underTest.scheduleTask(201, "schedule3");
    underTest.rescheduleTask(100, 204, "schedule1");
    assertEquals(List.of("schedule2"), objectStore.get(100L));
    assertEquals(List.of("schedule3", "schedule1"), objectStore.get(200L));
  }

  @Test
  void testCancelTask() {
    underTest.scheduleTask(100, "schedule1");
    underTest.scheduleTask(103, "schedule2");
    assertEquals(List.of("schedule1", "schedule2"), objectStore.get(100L));
    underTest.cancelTask(100, "schedule1");
    assertEquals(List.of("schedule2"), objectStore.get(100L));
  }

  @Test
  void testPunctuateNoYield() {
    underTest.scheduleTask(100, "schedule1");
    underTest.scheduleTask(200, "schedule2");
    underTest.scheduleTask(300, "schedule3");
    underTest.scheduleTask(400, "schedule4");
    underTest.scheduleTask(500, "schedule5");

    // two tasks will get rescheduled, and rest will finish in one iteration
    underTest.setReturnResult("schedule1", new RescheduleTaskResult(400));
    underTest.setReturnResult("schedule2", new CompletedTaskResult());
    underTest.setReturnResult("schedule3", new RescheduleTaskResult(600));
    underTest.setReturnResult("schedule4", new CompletedTaskResult());
    underTest.setReturnResult("schedule5", new CompletedTaskResult());

    underTest.punctuate(300);

    try (KeyValueIterator<Long, List<String>> it = objectStore.range(0L, 601L)) {
      while (it.hasNext()) {
        KeyValue<Long, List<String>> kv = it.next();
        switch (kv.key.intValue()) {
          case 400:
            assertEquals(List.of("schedule4", "schedule1"), kv.value);
            break;
          case 500:
            assertEquals(List.of("schedule5"), kv.value);
            break;
          case 600:
            assertEquals(List.of("schedule3"), kv.value);
            break;
          default:
            throw new RuntimeException("state expectation mismatch");
        }
      }
    }

    // finish up the two rescheduled tasks in next iteration
    underTest.setReturnResult("schedule1", new CompletedTaskResult());
    underTest.setReturnResult("schedule3", new CompletedTaskResult());

    underTest.punctuate(500);

    try (KeyValueIterator<Long, List<String>> it = objectStore.range(0L, 601L)) {
      while (it.hasNext()) {
        KeyValue<Long, List<String>> kv = it.next();
        if (kv.key.intValue() == 600) {
          assertEquals(List.of("schedule3"), kv.value);
        } else {
          throw new RuntimeException("state expectation mismatch");
        }
      }
    }

    underTest.punctuate(600);

    // all tasks done state store will be empty
    try (KeyValueIterator<Long, List<String>> it = objectStore.range(0L, 601L)) {
      assertFalse(it.hasNext());
    }
  }

  @Test
  void testPunctuateWithYield() {
    underTest.scheduleTask(100, "schedule1");
    underTest.scheduleTask(200, "schedule2.1");
    underTest.scheduleTask(200, "schedule2.2");
    underTest.scheduleTask(300, "schedule3");

    // all tasks complete in one iteration
    underTest.setReturnResult("schedule1", new CompletedTaskResult());
    underTest.setReturnResult("schedule2.1", new CompletedTaskResult());
    underTest.setReturnResult("schedule2.2", new CompletedTaskResult());
    underTest.setReturnResult("schedule3", new CompletedTaskResult());

    when(clock.millis())
        .thenReturn(0L) // startTimestamp storage
        .thenReturn(10L) // before first key 100
        .thenReturn(30L) // schedule1
        .thenReturn(60L) // before second key 200
        .thenReturn(90L) // schedule2.1
        .thenReturn(110L) // schedule2.2, crosses yield timeout of 100ms and bails out
        .thenAnswer(
            (inv) ->
                300L); // won't even be invoked for checks, one last will be done only for info log

    underTest.punctuate(301);

    try (KeyValueIterator<Long, List<String>> it = objectStore.range(0L, 601L)) {
      while (it.hasNext()) {
        KeyValue<Long, List<String>> kv = it.next();
        switch (kv.key.intValue()) {
          case 200:
            assertEquals(List.of("schedule2.2"), kv.value);
            break;
          case 300:
            assertEquals(List.of("schedule3"), kv.value);
            break;
          default:
            throw new RuntimeException("state expectation mismatch");
        }
      }
    }
  }

  class TestPunctuator extends AbstractThrottledPunctuator<String> {
    Map<String, TaskResult> resultMap = new HashMap<>();

    public TestPunctuator(
        Clock clock,
        ThrottledPunctuatorConfig config,
        KeyValueStore<Long, List<String>> objectStore) {
      super(clock, config, objectStore);
    }

    void setReturnResult(String object, TaskResult result) {
      resultMap.put(object, result);
    }

    @Override
    protected TaskResult executeTask(long punctuateTimestamp, String object) {
      return resultMap.get(object);
    }
  }
}
