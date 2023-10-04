package org.hypertrace.core.kafkastreams.framework.callbacks;

public class CallbackRegistryPunctuatorConfig {
  private final long yieldMs;
  private final long windowMs;

  public CallbackRegistryPunctuatorConfig(long yieldMs, long windowMs) {
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
