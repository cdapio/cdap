package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.util.ArrayList;
import java.util.List;

public class WordSplitter extends AbstractFlowlet {

  @UseDataSet("wordStats")
  private Table wordStatsTable;

  byte[] TOTALS_ROW = Bytes.toBytes("totals");
  byte[] TOTAL_LENGTH = Bytes.toBytes("total_length");
  byte[] TOTAL_WORDS = Bytes.toBytes("total_words");

  @Output("wordOut")
  private OutputEmitter<String> wordOutput;

  @Output("wordArrayOut")
  private OutputEmitter<List<String>> wordListOutput;

  public void process(StreamEvent event) throws OperationException {
    // Input is a String, need to split it by whitespace
    byte [] rawInput = Bytes.toBytes(event.getBody());
    String inputString = new String(rawInput);

    String [] words = inputString.split("\\s+");
    List<String> wordList = new ArrayList<String>(words.length);

    long sumOfLengths = 0;
    long wordCount = 0;

    // We have an array of words, now remove all non-alpha characters
    for (String word : words) {
      word = word.replaceAll("[^A-Za-z]", "");
      if (!word.isEmpty()) {
        // emit every word that remains
        wordOutput.emit(word);
        wordList.add(word);
        sumOfLengths += word.length();
        wordCount++;
      }
    }

    // Count other word statistics (word length, total words seen)
    this.wordStatsTable.write(
      new Increment(TOTALS_ROW,
                    new byte[][] { TOTAL_LENGTH,  TOTAL_WORDS },
                    new long[]   { sumOfLengths,  wordCount}));

    // Send the list of words to the associater
    wordListOutput.emit(wordList);
    
  }
}