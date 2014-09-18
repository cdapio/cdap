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
package co.cask.cdap.examples.countcounts;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process and increment Count.
 */
public class Incrementer extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(Incrementer
                                                               .class);

  @UseDataSet(CountCounts.TABLE_NAME)
  CountCounterTable counters;

  @ProcessInput
  public void process(Counts counts) {
    LOG.info(this.getContext().getName() + ": Received counts " + counts);

    // increment the word count (and count counts)
    this.counters.incrementWordCount(counts.getWordCount());

    // increment the line count
    this.counters.incrementLineCount();

    // increment the line length
    this.counters.incrementLineLength(counts.getLineLength());
  }
}
