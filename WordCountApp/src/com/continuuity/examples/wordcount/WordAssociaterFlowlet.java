package com.continuuity.examples.wordcount;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;

public class WordAssociaterFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        WordCountFlow.SPLITTER_ASSOCIATER_SCHEMA);
  }

  private WordAssocTable wordAssocTable;

  @Override
  public void initialize() {
    try {
      this.wordAssocTable = getFlowletContext().getDataSet("wordAssocs");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {
    String [] words = tuple.get("wordArray");
    
    try {

      // Store word associations
      Set<String> wordSet = new TreeSet<String>(Arrays.asList(words));
      this.wordAssocTable.writeWordAssocs(wordSet);

    } catch (OperationException e) {
      // Fail the process() call if we get an exception
      throw new RuntimeException(e);
    }
  }
}