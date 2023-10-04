package org.hypertrace.core.kafkastreams.framework.callbacks;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.kafkastreams.framework.callbacks.action.CallbackAction;

public class CallbackRegistryPunctuator<T> implements Punctuator {
  private final KeyValueStore<Long, List<T>> objectStore;
  private final CallbackRegistryPunctuatorConfig config;
  private final BiFunction<Long, T, CallbackAction> callbackFunction;

  public CallbackRegistryPunctuator(
      CallbackRegistryPunctuatorConfig config,
      KeyValueStore<Long, List<T>> objectStore,
      BiFunction<Long, T, CallbackAction> callbackFunction) {
    this.config = config;
    this.objectStore = objectStore;
    this.callbackFunction = callbackFunction;
  }

  public void add(long timestampInMs, T object) {
    long windowAlignedTimestamp = getWindowAlignedTimestamp(timestampInMs);
    List<T> objectsAtWindow =
        Optional.ofNullable(objectStore.get(windowAlignedTimestamp)).orElse(new ArrayList<>());
    objectsAtWindow.add(object);
    objectStore.put(windowAlignedTimestamp, objectsAtWindow);
  }

  public void drop(long atTimestampInMs, T object) {
    long windowAlignedTimestamp = getWindowAlignedTimestamp(atTimestampInMs);
    List<T> objectsAtWindow =
        Optional.ofNullable(objectStore.get(windowAlignedTimestamp)).orElse(new ArrayList<>());
    objectsAtWindow.remove(object);
    objectStore.put(windowAlignedTimestamp, objectsAtWindow);
  }

  @Override
  public void punctuate(long punctuateTimestamp) {
    long startTimestamp = System.currentTimeMillis();
    try (KeyValueIterator<Long, List<T>> it = objectStore.range(0L, punctuateTimestamp)) {
      while (it.hasNext() && canContinueProcessing(startTimestamp)) {
        KeyValue<Long, List<T>> kv = it.next();
        List<T> objects = kv.value;
        long windowAlignedTimestamp = kv.key;
        for (int i = 0; i < objects.size() && canContinueProcessing(startTimestamp); i++) {
          T object = objects.get(i);
          CallbackAction action = callbackFunction.apply(punctuateTimestamp, object);
          drop(windowAlignedTimestamp, object);
          if (action.getRescheduleTimestamp().isPresent()) {
            add(action.getRescheduleTimestamp().get(), object);
          }
        }
      }
    }
  }

  private boolean canContinueProcessing(long startTimestamp) {
    return System.currentTimeMillis() - startTimestamp < config.getYieldMs();
  }

  private long getWindowAlignedTimestamp(long timestamp) {
    return timestamp - (timestamp % config.getWindowMs());
  }
}
