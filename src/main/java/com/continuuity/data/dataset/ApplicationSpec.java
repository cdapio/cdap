package com.continuuity.data.dataset;

import java.util.LinkedList;
import java.util.List;

public class ApplicationSpec {

  private String name;
  private List<DataSetSpecification> datasets = new LinkedList<DataSetSpecification>();

  public ApplicationSpec dataset(DataSet dataset) {
    this.datasets.add(dataset.configure().create());
    return this;
  }

  public ApplicationSpec name(String name) {
    this.name = name;
    return this;
  }

  public List<DataSetSpecification> getDatasets() {
    return this.datasets;
  }

  public String getName() {
    return name;
  }
}
