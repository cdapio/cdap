package com.continuuity.data.dataset;

import java.util.LinkedList;
import java.util.List;

public class ApplicationSpec {

  private String name;
  private List<DataSetMeta> datasets = new LinkedList<DataSetMeta>();

  public ApplicationSpec dataset(DataSet dataset) {
    this.datasets.add(dataset.configure().create());
    return this;
  }

  public ApplicationSpec name(String name) {
    this.name = name;
    return this;
  }

  public List<DataSetMeta> getDatasets() {
    return this.datasets;
  }

  public String getName() {
    return name;
  }
}
