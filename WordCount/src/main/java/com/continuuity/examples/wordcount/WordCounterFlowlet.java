package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class WordCounterFlowlet extends AbstractFlowlet {

  public WordCounterFlowlet() {
    super("wordCounter");
  }

  @UseDataSet("wordStats")
  private Table wordStatsTable;
  
  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;

  byte[] TOTALS_ROW = Bytes.toBytes("totals");
  byte[] TOTAL_LENGTH = Bytes.toBytes("total_length");
  byte[] TOTAL_WORDS = Bytes.toBytes("total_words");

  private OutputEmitter<String> wordOutput;

  @ProcessInput("wordOut")
  public void process(String word) throws OperationException {
    // Count number of times we have seen this word
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
    
    // Count other word statistics (word length, total words seen)
    this.wordStatsTable.write(
        new Increment(TOTALS_ROW,
                      new byte[][] { TOTAL_LENGTH,  TOTAL_WORDS },
                      new long[]   { word.length(), 1L}));

    // Forward the word to the unique counter flowlet to do the unique count
    wordOutput.emit(word);
  }
}