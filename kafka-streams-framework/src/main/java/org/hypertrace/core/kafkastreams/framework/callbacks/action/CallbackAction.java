package org.hypertrace.core.kafkastreams.framework.callbacks.action;

import java.util.Optional;

public interface CallbackAction {
  Optional<Long> getRescheduleTimestamp();
}
