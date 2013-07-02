package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetSpecification;

/**
 * Throwing data-set. Constructor throws exception.
 */
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
