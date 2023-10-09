package org.hypertrace.core.kafkastreams.framework.punctuators;

public class ThrottledPunctuatorConfig {
  private final long yieldMs;
  private final long windowMs;

  public ThrottledPunctuatorConfig(long yieldMs, long windowMs) {
    this.yieldMs = yieldMs;
    this.windowMs = windowMs;
  }

  public long getYieldMs() {
    return yieldMs;
  }

  public long getWindowMs() {
    return windowMs;
  }
}
