/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

/**
 * Counter Flowlet.
 */
public class Counter extends AbstractFlowlet {
  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;
  private OutputEmitter<String> wordOutput;

  @ProcessInput("wordOut")
  public void process(String word) {
    
    // Count number of times we have seen this word
    this.wordCountsTable.increment(Bytes.toBytes(word), 1L);

    // Forward the word to the unique counter Flowlet to do the unique count
    wordOutput.emit(word);
  }
}
