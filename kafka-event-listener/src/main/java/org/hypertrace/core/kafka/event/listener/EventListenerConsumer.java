package org.hypertrace.core.kafka.event.listener;

public interface EventListenerConsumer {
  void startConsumer() throws Exception;

  void stopConsumer() throws Exception;
}
