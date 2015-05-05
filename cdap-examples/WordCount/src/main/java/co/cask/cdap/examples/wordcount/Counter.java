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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;

/**
 * Counter Flowlet.
 */
public class Counter extends AbstractFlowlet {
  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;
  private OutputEmitter<String> wordOutput;
  private Metrics metrics;
  int longestWordLength = 0;

  @ProcessInput("wordOut")
  public void process(String word) {
    // Count number of times we have seen this word
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
    if (word.length() > longestWordLength) {
      longestWordLength = word.length();
      metrics.gauge("longest.word.length", longestWordLength);
    }
    // Forward the word to the unique counter Flowlet to do the unique count
    wordOutput.emit(word);
  }
}
