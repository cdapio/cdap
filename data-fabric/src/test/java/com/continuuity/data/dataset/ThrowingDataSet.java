package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetSpecification;

/**
 * Throwing data-set. Constructor throws exception.
 */
public class ThrowingDataSet extends IncompleteDataSet {
  public ThrowingDataSet(String name) {
    super(name);
  }

  @Override
  public void initialize(DataSetSpecification spec) {
    super.initialize(spec);
    throw new IllegalArgumentException("don't ever call me!");
  }
}
