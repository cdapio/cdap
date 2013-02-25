package com.continuuity.examples.wordcount;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class WordAssociaterFlowlet extends AbstractFlowlet {

  @UseDataSet("wordAssocs")
  private WordAssocTable wordAssocTable;

  @ProcessInput("wordArrayOut")
  public void process(String [] words) throws OperationException {
    // Store word associations
    Set<String> wordSet = new TreeSet<String>(Arrays.asList(words));
    this.wordAssocTable.writeWordAssocs(wordSet);
  }
}