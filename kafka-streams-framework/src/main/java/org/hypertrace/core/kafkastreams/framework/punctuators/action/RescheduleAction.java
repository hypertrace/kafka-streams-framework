package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class RescheduleAction implements ScheduleAction {
  private final long rescheduleTimestamp;

  public RescheduleAction(long rescheduleTimestamp) {
    this.rescheduleTimestamp = rescheduleTimestamp;
  }

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.of(rescheduleTimestamp);
  }
}
