package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetSpecification;

// this class' data set spec constructor throws an exception
public class ThrowingDataSet extends IncompleteDataSet {
  public ThrowingDataSet(String name) {
    super(name);
  }
  @SuppressWarnings("unused")
  public ThrowingDataSet(DataSetSpecification spec) {
    super(spec.getName());
    throw new IllegalArgumentException("don't ever call me!");
  }
}
