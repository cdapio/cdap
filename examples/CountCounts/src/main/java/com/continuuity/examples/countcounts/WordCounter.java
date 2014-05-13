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
package com.continuuity.examples.countcounts;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts words.
 */
public class WordCounter extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(WordCounter
                                                               .class);

  private OutputEmitter<Counts> output;

  @ProcessInput
  public void process(String line) {
    LOG.info(this.getContext().getName() + ": Received line " + line);

    // Count the number of words
    final String delimiters = "[ .-]";
    int wordCount = 0;
    if (line != null) {
      wordCount = line.split(delimiters).length;
    }

    // Count the total length
    int lineLength = line.length();

    LOG.info(this.getContext().getName() + ": Emitting count " + wordCount +
                " and length " + lineLength);

    output.emit(new Counts(wordCount, lineLength));
  }

}
