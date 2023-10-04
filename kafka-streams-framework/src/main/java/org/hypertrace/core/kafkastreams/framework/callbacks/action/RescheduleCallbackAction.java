package org.hypertrace.core.kafkastreams.framework.callbacks.action;

import java.util.Optional;

public class RescheduleCallbackAction implements CallbackAction {
  private final long rescheduleTimestamp;

  public RescheduleCallbackAction(long rescheduleTimestamp) {
    this.rescheduleTimestamp = rescheduleTimestamp;
  }

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.of(rescheduleTimestamp);
  }
}
