/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.examples.wordcount;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Word splitter Flowlet.
 */
public class WordSplitter extends AbstractFlowlet {
  private Table wordStatsTable;
  @Output("wordOut")
  private OutputEmitter<String> wordOutput;
  @Output("wordArrayOut")
  private OutputEmitter<List<String>> wordListOutput;

  @Property
  private final String wordStatsTableName;

  public WordSplitter(String wordStatsTableName) {
    this.wordStatsTableName = wordStatsTableName;
  }

  @Override
  public void configure(FlowletConfigurer configurer) {
    super.configure(configurer);
    useDatasets(wordStatsTableName);
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    wordStatsTable = context.getDataset(wordStatsTableName);
  }

  @ProcessInput
  public void process(StreamEvent event) {

    // Input is a String, need to split it by whitespace
    String inputString = Charset.forName("UTF-8")
      .decode(event.getBody()).toString();

    String[] words = inputString.split("\\s+");
    List<String> wordList = new ArrayList<>(words.length);

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
    this.wordStatsTable.increment(new Increment("totals")
                                          .add("total_length", sumOfLengths)
                                          .add("total_words", wordCount));

    // Send the list of words to the associater
    wordListOutput.emit(wordList);

  }
}
