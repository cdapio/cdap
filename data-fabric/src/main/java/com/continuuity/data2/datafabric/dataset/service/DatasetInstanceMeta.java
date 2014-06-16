package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;

/**
 * Dataset instance metadata.
 */
public class DatasetInstanceMeta {
  private final DatasetSpecification spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta type;

  public DatasetInstanceMeta(DatasetSpecification spec, DatasetTypeMeta type) {
    this.spec = spec;
    this.type = type;
  }

  public DatasetSpecification getSpec() {
    return spec;
  }

  public DatasetTypeMeta getType() {
    return type;
  }
}
