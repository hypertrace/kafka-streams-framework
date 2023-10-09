package org.hypertrace.core.kafkastreams.framework.punctuators;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.ScheduleAction;

@Slf4j
public abstract class AbstractThrottledPunctuator<T> implements Punctuator {
  private final Clock clock;
  private final KeyValueStore<Long, ArrayList<T>> objectStore;
  private final ThrottledPunctuatorConfig config;

  public AbstractThrottledPunctuator(
      Clock clock,
      ThrottledPunctuatorConfig config,
      KeyValueStore<Long, ArrayList<T>> objectStore) {
    this.clock = clock;
    this.config = config;
    this.objectStore = objectStore;
  }

  public void scheduleTask(long atTimestampInMs, T object) {
    long windowAlignedTimestamp = getWindowAlignedTimestamp(atTimestampInMs);
    ArrayList<T> objectsAtWindow =
        Optional.ofNullable(objectStore.get(windowAlignedTimestamp)).orElse(new ArrayList<>());
    objectsAtWindow.add(object);
    objectStore.put(windowAlignedTimestamp, objectsAtWindow);
  }

  public boolean rescheduleTask(long atTimestampInMs, long toTimestampInMs, T object) {
    if (cancelTask(atTimestampInMs, object)) {
      scheduleTask(toTimestampInMs, object);
      return true;
    } else {
      return false;
    }
  }

  public boolean cancelTask(long atTimestampInMs, T object) {
    long windowAlignedTimestamp = getWindowAlignedTimestamp(atTimestampInMs);
    ArrayList<T> objectsAtWindow =
        Optional.ofNullable(objectStore.get(windowAlignedTimestamp)).orElse(new ArrayList<>());
    boolean removed = objectsAtWindow.remove(object);
    if (objectsAtWindow.isEmpty()) {
      objectStore.delete(windowAlignedTimestamp);
    } else {
      objectStore.put(windowAlignedTimestamp, objectsAtWindow);
    }
    return removed;
  }

  @Override
  public final void punctuate(long punctuateTimestamp) {
    long startTimestamp = System.currentTimeMillis();
    log.debug(
        "Processing tasks with throttling yield of {} until timestamp {}",
        config.getYieldMs(),
        punctuateTimestamp);
    try (KeyValueIterator<Long, ArrayList<T>> it = objectStore.range(0L, punctuateTimestamp)) {
      while (it.hasNext() && canContinueProcessing(startTimestamp)) {
        KeyValue<Long, ArrayList<T>> kv = it.next();
        List<T> objects = kv.value;
        long windowAlignedTimestamp = kv.key;
        for (int i = 0; i < objects.size() && canContinueProcessing(startTimestamp); i++) {
          T object = objects.get(i);
          ScheduleAction action = callback(punctuateTimestamp, object);
          if (!cancelTask(windowAlignedTimestamp, object)) {
            log.debug(
                "Failed to cancel task at key {}, not found in object store at expected window {}",
                object,
                windowAlignedTimestamp);
          }
          action
              .getRescheduleTimestamp()
              .ifPresent((rescheduleTimestamp) -> scheduleTask(rescheduleTimestamp, object));
        }
      }
    }
  }

  protected abstract ScheduleAction callback(long punctuateTimestamp, T object);

  private boolean canContinueProcessing(long startTimestamp) {
    return clock.millis() - startTimestamp < config.getYieldMs();
  }

  private long getWindowAlignedTimestamp(long timestamp) {
    return timestamp - (timestamp % config.getWindowMs());
  }
}
