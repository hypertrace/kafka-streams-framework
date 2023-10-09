package org.hypertrace.core.kafkastreams.framework.punctuators.action;

import java.util.Optional;

public interface ScheduleAction {
  Optional<Long> getRescheduleTimestamp();
}
