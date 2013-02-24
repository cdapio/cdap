package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

public class UniqueCounterFlowlet extends AbstractFlowlet {

  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;

  public UniqueCounterFlowlet() {
    super("uniqueCounter");
  }

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription("Example Word Count Procedure")
      .useDataSet("uniqueCount")
      .build();
  }

  public void process(String word) throws OperationException {
    this.uniqueCountTable.updateUniqueCount(word);
  }
}