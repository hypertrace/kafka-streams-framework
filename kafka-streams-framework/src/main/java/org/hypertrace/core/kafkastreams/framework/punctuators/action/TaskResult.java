package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public interface TaskResult {
  Optional<Long> getRescheduleTimestamp();
}
