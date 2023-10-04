package org.hypertrace.core.kafkastreams.framework.callbacks.action;

import java.util.Optional;

public class DropCallbackAction implements CallbackAction {

  @Override
  public Optional<Long> getRescheduleTimestamp() {
    return Optional.empty();
  }
}
