package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class WordAssociator extends AbstractFlowlet {
  @UseDataSet("wordAssocs")
  private AssociationTable associationTable;

  public void process(String [] words) throws OperationException {
    // Store word associations
    Set<String> wordSet = new TreeSet<String>(Arrays.asList(words));
    this.associationTable.writeWordAssocs(wordSet);
  }
}