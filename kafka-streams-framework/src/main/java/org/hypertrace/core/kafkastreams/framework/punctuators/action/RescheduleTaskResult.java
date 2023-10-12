package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class RescheduleTaskResult implements TaskResult {
  private final long rescheduleTimestamp;

  public RescheduleTaskResult(long rescheduleTimestamp) {
    this.rescheduleTimestamp = rescheduleTimestamp;
  }

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.of(rescheduleTimestamp);
  }
}
