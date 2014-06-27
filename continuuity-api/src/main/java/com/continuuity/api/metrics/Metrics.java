package com.continuuity.api.metrics;

/**
 * Defines a way to collect user-defined metrics.
 * To use it, just add a Metrics field in a Reactor application element, for example a flowlet, and start using it.
 */
public interface Metrics {
  /**
   * Increases the value of the specific counter by delta.
   * @param counterName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  void count(String counterName, int delta);
}

