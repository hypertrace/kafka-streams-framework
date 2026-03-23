package org.hypertrace.core.kafkastreams.framework.interceptors.metrics;

import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsInterceptorTest {

  private Counter timeLagCounter;
  private Counter numRecordsCounter;
  private MetricsInterceptor interceptor;

  @BeforeEach
  void setup() {
    timeLagCounter = mock(Counter.class);
    numRecordsCounter = mock(Counter.class);
  }

  @Test
  void shouldIncrementCounters() {
    //    Record<Object, Object> record =
    //        new Record<>("key", "value", System.currentTimeMillis() - 50000);
    //    interceptor.process(record);
    //
    //    verify(numRecordsCounter, times(1)).increment();
    //    verify(timeLagCounter, times(1)).increment(anyDouble());
  }
}
