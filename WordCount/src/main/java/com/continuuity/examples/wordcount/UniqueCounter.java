package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class UniqueCounter extends AbstractFlowlet {

  public UniqueCounter() {
    super("uniqueCounter");
  }

  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;

  public void process(String word) throws OperationException {
    this.uniqueCountTable.updateUniqueCount(word);
  }
}