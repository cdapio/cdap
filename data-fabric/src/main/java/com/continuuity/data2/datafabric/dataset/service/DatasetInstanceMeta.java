package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;

/**
 * Dataset instance metadata.
 */
public class DatasetInstanceMeta {
  private final DatasetInstanceSpec spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta type;

  public DatasetInstanceMeta(DatasetInstanceSpec spec, DatasetTypeMeta type) {
    this.spec = spec;
    this.type = type;
  }

  public DatasetInstanceSpec getSpec() {
    return spec;
  }

  public DatasetTypeMeta getType() {
    return type;
  }
}
