package org.hypertrace.core.kafkastreams.framework.interceptors;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class StreamsBuilderWithInterceptor extends StreamsBuilder {

  private final StreamsBuilder delegate;
  private final List<ProcessorSupplier> processorSuppliers;

  public StreamsBuilderWithInterceptor(
      StreamsBuilder delegate, List<ProcessorSupplier> processorSuppliers) {
    this.delegate = delegate;
    this.processorSuppliers = processorSuppliers;
  }

  @Override
  public synchronized Topology build() {
    return delegate.build();
  }

  @Override
  public synchronized <K, V> KStream<K, V> stream(
      final Pattern topicPattern, final Consumed<K, V> consumed) {
    KStream<K, V> stream = delegate.stream(topicPattern, consumed);
    processorSuppliers.forEach(stream::process);
    return stream;
  }

  @Override
  public synchronized <K, V> KStream<K, V> stream(
      final Collection<String> topics, final Consumed<K, V> consumed) {
    KStream<K, V> stream = delegate.stream(topics, consumed);
    processorSuppliers.forEach(stream::process);
    return stream;
  }
}
