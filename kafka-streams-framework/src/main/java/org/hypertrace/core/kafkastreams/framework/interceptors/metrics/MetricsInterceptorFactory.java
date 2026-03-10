package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import io.micrometer.core.instrument.Counter;
import java.util.Collections;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class MetricsInterceptorFactory {
  private static final String TIME_LAG_COUNTER_NAME = "time_lag";
  private static final String NUM_RECORDS_COUNTER_NAME = "num_records";

  private final Counter timeLagCounter;
  private final Counter numRecordsCounter;

  public MetricsInterceptorFactory() {
    this.numRecordsCounter =
        PlatformMetricsRegistry.registerCounter(NUM_RECORDS_COUNTER_NAME, Collections.emptyMap());
    this.timeLagCounter =
        PlatformMetricsRegistry.registerCounter(TIME_LAG_COUNTER_NAME, Collections.emptyMap());
  }

  public MetricsInterceptor create() {
    return new MetricsInterceptor(numRecordsCounter, timeLagCounter);
  }
}
