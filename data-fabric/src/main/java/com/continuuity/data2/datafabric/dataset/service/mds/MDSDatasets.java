package com.continuuity.data2.datafabric.dataset.service.mds;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.data2.dataset2.tx.TxContext;

import java.util.Map;

/**
 * Provides transactional access to datasets storing metadata of datasets
 */
public final class MDSDatasets extends TxContext {
  MDSDatasets(Map<String, ? extends Dataset> datasets) {
    super(datasets);
  }

  public DatasetInstanceMDS getInstanceMDS() {
    return (DatasetInstanceMDS) getDataset("datasets.instance");
  }

  public DatasetTypeMDS getTypeMDS() {
    return (DatasetTypeMDS) getDataset("datasets.type");
  }
}
