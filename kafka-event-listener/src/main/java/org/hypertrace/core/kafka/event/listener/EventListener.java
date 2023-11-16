package org.hypertrace.core.kafka.event.listener;

import java.util.ArrayList;
import java.util.List;

public abstract class EventListener {
  private final List<EventListenerConsumer> eventListenerConsumers = new ArrayList<>();

  public <D, C> void registerConsumer(EventListenerConsumer eventListenerConsumer)
      throws Exception {
    eventListenerConsumer.startConsumer();
    eventListenerConsumers.add(eventListenerConsumer);
  }

  /** this method needs to be concurrent safe if registering multiple consumers */
  public abstract <D, C> void actOnEvent(D eventKey, C eventValue);

  public void close() {
    eventListenerConsumers.forEach(
        eventListenerConsumer -> {
          try {
            eventListenerConsumer.stopConsumer();
          } catch (Exception e) {

          }
        });
  }
}
