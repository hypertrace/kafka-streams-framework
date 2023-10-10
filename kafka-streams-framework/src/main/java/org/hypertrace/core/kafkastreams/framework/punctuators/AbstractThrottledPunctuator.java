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
import org.hypertrace.core.kafkastreams.framework.punctuators.action.TaskResult;

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
    long startTimestamp = clock.millis();
    log.debug(
        "Processing tasks with throttling yield of {} until timestamp {}",
        config.getYieldMs(),
        punctuateTimestamp);
    int keyCounter = 0;
    int taskCounter = 0;
    try (KeyValueIterator<Long, ArrayList<T>> it =
        objectStore.range(getRangeStart(punctuateTimestamp), getRangeEnd(punctuateTimestamp))) {
      while (it.hasNext() && canContinueProcessing(startTimestamp)) {
        KeyValue<Long, ArrayList<T>> kv = it.next();
        keyCounter++;
        List<T> objects = kv.value;
        long windowAlignedTimestamp = kv.key;
        for (int i = 0; i < objects.size() && canContinueProcessing(startTimestamp); i++) {
          T object = objects.get(i);
          taskCounter++;
          TaskResult action = executeTask(punctuateTimestamp, object);
          action
              .getRescheduleTimestamp()
              .ifPresent((rescheduleTimestamp) -> scheduleTask(rescheduleTimestamp, object));
          if (!cancelTask(windowAlignedTimestamp, object)) {
            log.debug(
                "Failed to cancel task at key {}, not found in object store at expected window {}",
                object,
                windowAlignedTimestamp);
          }
        }
      }
    }
    log.debug("Executed {} tasks in total from {} store keys", taskCounter, keyCounter);
  }

  protected abstract TaskResult executeTask(long punctuateTimestamp, T object);

  protected long getRangeStart(long punctuateTimestamp) {
    return 0;
  }

  protected long getRangeEnd(long punctuateTimestamp) {
    return punctuateTimestamp;
  }

  private boolean canContinueProcessing(long startTimestamp) {
    return clock.millis() - startTimestamp < config.getYieldMs();
  }

  private long getWindowAlignedTimestamp(long timestamp) {
    return timestamp - (timestamp % config.getWindowMs());
  }
}
