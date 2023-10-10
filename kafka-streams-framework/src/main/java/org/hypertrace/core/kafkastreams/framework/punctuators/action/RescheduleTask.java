package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class RescheduleTask implements TaskResult {
  private final long rescheduleTimestamp;

  public RescheduleTask(long rescheduleTimestamp) {
    this.rescheduleTimestamp = rescheduleTimestamp;
  }

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.of(rescheduleTimestamp);
  }
}
