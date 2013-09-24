package com.continuuity.api.metrics;

/**
 * Defines a way to collect metrics.
 * To use it, just add a Metrics field in the component managed by AppFabric (e.g. Flowlet) and start using it.
 */
public interface Metrics {
  /**
   * Increases the value of the specific counter by delta.
   * @param counterName Name of the counter.
   * @param delta The value to increase by.
   */
  void count(String counterName, int delta);
}
