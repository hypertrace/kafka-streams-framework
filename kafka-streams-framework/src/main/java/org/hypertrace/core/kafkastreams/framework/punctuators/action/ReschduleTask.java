package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class ReschduleTask implements TaskResult {
  private final long rescheduleTimestamp;

  public ReschduleTask(long rescheduleTimestamp) {
    this.rescheduleTimestamp = rescheduleTimestamp;
  }

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.of(rescheduleTimestamp);
  }
}
