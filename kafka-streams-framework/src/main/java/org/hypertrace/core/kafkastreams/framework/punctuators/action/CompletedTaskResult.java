package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public class CompletedTaskResult implements TaskResult {

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.empty();
  }
}
