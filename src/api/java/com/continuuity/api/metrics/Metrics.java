package com.continuuity.api.metrics;

/**
 * Defines a way to collect metrics.
 * To use it, just add Metrics field in the component managed by AppFabric (e.g. Flowlet) and start using it.
 */
public interface Metrics {
  /**
   * Increases the value of the specific counter by delta.
   * @param counterName name of the counter
   * @param delta the value to increase by
   */
  void count(String counterName, int delta);
}
