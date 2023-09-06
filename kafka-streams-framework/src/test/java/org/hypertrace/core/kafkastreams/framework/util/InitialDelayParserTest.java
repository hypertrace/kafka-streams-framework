package org.hypertrace.core.kafkastreams.framework.util;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class InitialDelayParserTest {

  @Test()
  void test_getInitialDelay_noVersionChange() {
    Duration initialDelay =
        InitialDelayParser.getInstance().getInitialDelay(Collections.emptyMap());
    assertEquals(Duration.ofMillis(0L), initialDelay);

    initialDelay = InitialDelayParser.getInstance().getInitialDelay(Map.of("initial.delay", "10m"));
    assertEquals(Duration.ofMinutes(10L), initialDelay);
  }

  @Test()
  @SetEnvironmentVariable(key = "SERVICE_VERSION", value = "2.0.0")
  void test_getInitialDelay_majorVersionChange() {
    Duration initialDelay =
        InitialDelayParser.getInstance().getInitialDelay(Collections.emptyMap());
    assertEquals(Duration.ofMinutes(6L), initialDelay);

    initialDelay = InitialDelayParser.getInstance().getInitialDelay(Map.of("initial.delay", "10m"));
    assertEquals(Duration.ofMinutes(10L), initialDelay);
  }

  @Test()
  @SetEnvironmentVariable(key = "SERVICE_VERSION", value = "2.1.0")
  void test_getInitialDelay_minorVersionChange() {
    Duration initialDelay =
        InitialDelayParser.getInstance().getInitialDelay(Collections.emptyMap());
    assertEquals(Duration.ofMillis(0L), initialDelay);

    initialDelay = InitialDelayParser.getInstance().getInitialDelay(Map.of("initial.delay", "10m"));
    assertEquals(Duration.ofMinutes(10L), initialDelay);
  }

  @Test()
  @SetEnvironmentVariable(key = "SERVICE_VERSION", value = "2.1.1")
  void test_getInitialDelay_patchVersionChange() {
    Duration initialDelay =
        InitialDelayParser.getInstance().getInitialDelay(Collections.emptyMap());
    assertEquals(Duration.ofMillis(0L), initialDelay);

    initialDelay = InitialDelayParser.getInstance().getInitialDelay(Map.of("initial.delay", "10m"));
    assertEquals(Duration.ofMinutes(10L), initialDelay);
  }
}
