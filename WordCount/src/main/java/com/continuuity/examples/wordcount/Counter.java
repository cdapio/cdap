package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class Counter extends AbstractFlowlet {
  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;

  private OutputEmitter<String> wordOutput;

  @ProcessInput("wordOut")
  public void process(String word) throws OperationException {
    // Count number of times we have seen this word
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
    
    // Forward the word to the unique counter flowlet to do the unique count
    wordOutput.emit(word);
  }
}