package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;

public abstract class DataSet {

  private final String name;
  public String getName() {
    return this.name;
  }

  public DataSet(String name) {
    this.name = name;
  }

  abstract void initialize(DataSetMeta meta) throws OperationException;
  abstract DataSetMeta.Builder configure();
}
