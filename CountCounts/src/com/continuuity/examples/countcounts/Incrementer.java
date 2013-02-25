package com.continuuity.examples.countcounts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class Incrementer extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(Incrementer.class);

  @UseDataSet(CountCounts.tableName)
  CountCounterTable counters;

  public void process(Counts counts) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received counts " + counts);

    // increment the word count (and count counts)
    this.counters.incrementWordCount(counts.getWordCount());

    // increment the line count
    this.counters.incrementLineCount();
    
    // increment the line length
    this.counters.incrementLineLength(counts.getLineLength());
  }
}
