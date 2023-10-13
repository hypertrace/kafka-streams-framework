package org.hypertrace.core.kafkastreams.framework.punctuators;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
  private final KeyValueStore<Long, ArrayList<T>> eventStore;
  private final ThrottledPunctuatorConfig config;

  public AbstractThrottledPunctuator(
      Clock clock, ThrottledPunctuatorConfig config, KeyValueStore<Long, ArrayList<T>> eventStore) {
    this.clock = clock;
    this.config = config;
    this.eventStore = eventStore;
  }

  public void scheduleTask(long scheduleMs, T event) {
    long windowMs = normalize(scheduleMs);
    ArrayList<T> events = Optional.ofNullable(eventStore.get(windowMs)).orElse(new ArrayList<>());
    events.add(event);
    eventStore.put(windowMs, events);
  }

  public boolean rescheduleTask(long oldScheduleMs, long newScheduleMs, T event) {
    scheduleTask(newScheduleMs, event);
    return cancelTask(oldScheduleMs, event);
  }

  public boolean cancelTask(long scheduleMs, T event) {
    long windowMs = normalize(scheduleMs);
    ArrayList<T> events = Optional.ofNullable(eventStore.get(windowMs)).orElse(new ArrayList<>());
    boolean removed = events.remove(event);
    if (removed) {
      if (events.isEmpty()) {
        eventStore.delete(windowMs);
      } else {
        eventStore.put(windowMs, events);
      }
    } else {
      log.warn(
          "task cancel failed. event not found for ts: {}, window: {}",
          new Date(scheduleMs),
          new Date(windowMs));
    }
    return removed;
  }

  @Override
  public final void punctuate(long timestamp) {
    long startTime = clock.millis();
    int totalProcessedWindows = 0;
    int totalProcessedTasks = 0;

    log.debug(
        "Processing tasks with throttling yield of {} until timestamp {}",
        config.getYieldMs(),
        timestamp);
    try (KeyValueIterator<Long, ArrayList<T>> it =
        eventStore.range(getRangeStart(timestamp), getRangeEnd(timestamp))) {
      // iterate through all keys in range until yield timeout is reached
      while (it.hasNext() && !shouldYieldNow(startTime)) {
        KeyValue<Long, ArrayList<T>> kv = it.next();
        totalProcessedWindows++;
        ArrayList<T> events = kv.value;
        long windowMs = kv.key;
        // collect all tasks to be rescheduled by key to perform bulk reschedules
        Map<Long, ArrayList<T>> rescheduledTasks = new HashMap<>();
        // loop through all events for this key until yield timeout is reached
        int i = 0;
        for (; i < events.size() && !shouldYieldNow(startTime); i++) {
          T event = events.get(i);
          totalProcessedTasks++;
          TaskResult action = executeTask(timestamp, event);
          action
              .getRescheduleTimestamp()
              .ifPresent(
                  (rescheduleTimestamp) ->
                      rescheduledTasks
                          .computeIfAbsent(normalize(rescheduleTimestamp), (t) -> new ArrayList<>())
                          .add(event));
        }
        // process all reschedules
        rescheduledTasks.forEach(
            (newWindowMs, rescheduledEvents) -> {
              ArrayList<T> windowTasks =
                  Optional.ofNullable(eventStore.get(newWindowMs)).orElse(new ArrayList<>());
              windowTasks.addAll(rescheduledEvents);
              eventStore.put(newWindowMs, windowTasks);
            });

        // all tasks till i-1 have been cancelled or rescheduled hence to be removed from store
        if (i == events.size()) {
          // can directly delete key from store
          eventStore.delete(windowMs);
        } else {
          eventStore.put(windowMs, new ArrayList<>(events.subList(i, events.size())));
        }
      }
    }
    log.info(
        "processed windows: {}, processed tasks: {}, time taken: {}",
        totalProcessedWindows,
        totalProcessedTasks,
        clock.millis() - startTime);
  }

  protected abstract TaskResult executeTask(long punctuateTimestamp, T object);

  protected long getRangeStart(long punctuateTimestamp) {
    return 0;
  }

  protected long getRangeEnd(long punctuateTimestamp) {
    return punctuateTimestamp;
  }

  private boolean shouldYieldNow(long startTimestamp) {
    return (clock.millis() - startTimestamp > config.getYieldMs());
  }

  private long normalize(long timestamp) {
    return timestamp - (timestamp % config.getWindowMs());
  }
}
