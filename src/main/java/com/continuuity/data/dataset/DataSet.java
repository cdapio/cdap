package com.continuuity.data.dataset;

public abstract class DataSet {

  private final String name;
  public final String getName() {
    return this.name;
  }

  public DataSet(String name) {
    this.name = name;
  }

  public DataSet(DataSetMeta meta) {
    this.name = meta.getName();
  }

  abstract DataSetMeta.Builder configure();
}
