package org.hypertrace.core.kafkastreams.framework.async;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Encapsulates the {@link Record record} and the {@link String childName} used by the {@link
 * AsyncProcessor} to forward messages to the downstream processor
 *
 * @param <K> type of the record key
 * @param <V> type of the record value
 */
@AllArgsConstructor
@Getter
public class RecordToForward<K, V> {
  @Nullable private String childName;
  private Record<K, V> record;

  public static <K, V> RecordToForward<K, V> from(
      String childName, K key, V value, long timestamp) {
    return new RecordToForward<>(childName, new Record<>(key, value, timestamp));
  }
}
