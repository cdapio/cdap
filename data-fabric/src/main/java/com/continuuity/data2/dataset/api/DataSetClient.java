package com.continuuity.data2.dataset.api;

/**
 * Defines interface for DataSet client.
 */
public interface DataSetClient {
  void close();

  // these are to support current metrics UI. TODO: should be revised
  void setMetricsCollector(DataOpsMetrics dataOpsMetrics);

  /**
   * Collector for data ops metrics.
   */
  static interface DataOpsMetrics {
    void recordRead(int opsCount);
    void recordWrite(int opsCount, int dataSize);
  }
}
