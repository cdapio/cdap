package com.continuuity.examples.countcounts;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounter extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);

  private OutputEmitter<Counts> output;

  public void process(String line) {
    LOG.debug(this.getContext().getName() + ": Received line " + line);

    // Count the number of words
    final String delimiters = "[ .-]";
    int wordCount = 0;
    if (line != null) {
      wordCount = line.split(delimiters).length;
    }

    // Count the total length
    int lineLength = line.length();

    LOG.debug(this.getContext().getName() + ": Emitting count " + wordCount +
        " and length " + lineLength);

    output.emit(new Counts(wordCount, lineLength));
  }

}
