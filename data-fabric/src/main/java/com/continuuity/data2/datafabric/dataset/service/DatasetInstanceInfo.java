package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;

/**
 * Dataset instance information.
 */
public class DatasetInstanceInfo {
  private final DatasetInstanceSpec spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta typeMeta;

  public DatasetInstanceInfo(DatasetInstanceSpec spec, DatasetTypeMeta typeMeta) {
    this.spec = spec;
    this.typeMeta = typeMeta;
  }

  public DatasetInstanceSpec getSpec() {
    return spec;
  }

  public DatasetTypeMeta getTypeMeta() {
    return typeMeta;
  }
}
