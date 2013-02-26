package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.util.ArrayList;
import java.util.List;

public class WordSplitter extends AbstractFlowlet {

  @Output("wordOut")
  private OutputEmitter<String> wordOutput;

  @Output("wordArrayOut")
  private OutputEmitter<List<String>> wordListOutput;

  public void process(StreamEvent event) {
    // Input is a String, need to split it by whitespace
    byte [] rawInput = Bytes.toBytes(event.getBody());
    String inputString = new String(rawInput);

    String [] words = inputString.split("\\s+");
    List<String> wordList = new ArrayList<String>(words.length);

    // We have an array of words, now remove all non-alpha characters
    for (String word : words) {
      word = word.replaceAll("[^A-Za-z]", "");
      if (!word.isEmpty()) {
        // emit every word that remains
        wordOutput.emit(word);
        wordList.add(word);
      }
    }

    // Send the list of words to the associater
    wordListOutput.emit(wordList);
    
  }
}