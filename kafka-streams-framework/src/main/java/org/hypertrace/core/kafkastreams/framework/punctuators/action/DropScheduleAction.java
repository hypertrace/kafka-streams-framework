package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class DropScheduleAction implements ScheduleAction {

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.empty();
  }
}
