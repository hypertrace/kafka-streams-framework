package org.hypertrace.core.kafkastreams.framework.callbacks;

import java.util.Optional;

public class DropCallbackAction implements CallbackAction {

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.empty();
  }
}
