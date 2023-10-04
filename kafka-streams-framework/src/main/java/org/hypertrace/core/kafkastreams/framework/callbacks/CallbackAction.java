package org.hypertrace.core.kafkastreams.framework.callbacks;

import java.util.Optional;

public interface CallbackAction {
  Optional<Long> getRescheduleTimestamp();
}
