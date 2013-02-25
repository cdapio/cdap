package com.continuuity.examples.countrandom;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class NumberCounter extends AbstractFlowlet {

  @UseDataSet(CountRandom.tableName)
  KeyValueTable counters;

  public void process(Integer number) throws OperationException {
    counters.increment(number.toString().getBytes(), 1L);
  }
}