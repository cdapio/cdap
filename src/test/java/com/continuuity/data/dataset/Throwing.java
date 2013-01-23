package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetSpecification;

// this class' data set spec constructor throws an exception
public class Throwing extends IncompleteDataSet {
  public Throwing(String name) {
    super(name);
  }
  @SuppressWarnings("unused")
  public Throwing(DataSetSpecification spec) {
    super(spec.getName());
    throw new IllegalArgumentException("don't ever call me!");
  }
}
