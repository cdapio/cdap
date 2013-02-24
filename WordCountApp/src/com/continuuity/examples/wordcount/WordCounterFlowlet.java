package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class WordCounterFlowlet extends AbstractFlowlet {

  @UseDataSet("wordStats")
  private Table wordStatsTable;

  @UseDataSet("wordCounts")
  private Table wordCountsTable;

  private OutputEmitter<String> wordOutput;

  public WordCounterFlowlet() {
    super("wordCounter");
  }

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription("Example Word Count Procedure")
      .useDataSet("wordStats","wordCounts")
      .build();
  }

  @Process("wordOut")
  public void process(String word) throws OperationException {
    // Count number of times we have seen this word
    this.wordCountsTable.write(
        new Increment(Bytes.toBytes(word), Bytes.toBytes("total"), 1L));
    
    // Count other word statistics (word length, total words seen)
    this.wordStatsTable.write(
        new Increment(Bytes.toBytes("totals"),
            new byte [][] {
              Bytes.toBytes("total_length"), Bytes.toBytes("total_words")
            },
            new long [] { word.length(), 1L }));

    // Send the word to the unique counter flowlet to do the unique count
    wordOutput.emit(word);
  }
}