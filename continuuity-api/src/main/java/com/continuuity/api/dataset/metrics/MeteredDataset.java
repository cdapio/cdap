package com.continuuity.api.dataset.metrics;

import com.continuuity.api.annotation.Beta;

/**
 * Defines interface to be implemented by {@link com.continuuity.api.dataset.Dataset} implementations to
 * expose data ops metrics.
 */
@Beta
public interface MeteredDataset {
  /**
   * Sets data ops metrics collector
   * @param metricsCollector metrics collector
   */
  void setMetricsCollector(MetricsCollector metricsCollector);

  /**
   * Collector for data ops metrics.
   */
  static interface MetricsCollector {

    /**
     * Records read operation stats
     * @param opsCount number of ops performed
     * @param dataSizeInBytes size of data read
     */
    void recordRead(int opsCount, int dataSizeInBytes);

    /**
     * Records write operation stats
     * @param opsCount number of ops performed
     * @param dataSizeInBytes size of data written
     */
    void recordWrite(int opsCount, int dataSizeInBytes);
  }
}
