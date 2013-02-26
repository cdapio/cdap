package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.Set;

public class WordAssociator extends AbstractFlowlet {
  @UseDataSet("wordAssocs")
  private AssociationTable associationTable;

  public void process(Set<String> words) throws OperationException {
    // Store word associations
    this.associationTable.writeWordAssocs(words);
  }
}