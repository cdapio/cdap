package com.continuuity.data2.dataset2.tx;

import com.continuuity.api.dataset.Dataset;

import java.util.Map;

/**
 *
 */
public abstract class TxContext {
  private final Map<String, ? extends Dataset> datasets;

  protected TxContext(Map<String, Dataset> datasets) {
    this.datasets = datasets;
  }

  protected <T extends Dataset> T getDataset(String name) {
    return (T) datasets.get(name);
  }

  Map<String, ? extends Dataset> getDatasets() {
    return datasets;
  }
}
