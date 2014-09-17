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
package co.cask.cdap.examples.countandfilterwords;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Counter Flowlet; increments word count.
 */
public class Counter extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(Counter.class);

  @UseDataSet(CountAndFilterWords.TABLE_NAME)
  KeyValueTable counters;

  private Metrics metric;

  @ProcessInput("counts")
  public void process(String counter) {
    LOG.info("Incrementing counter " + counter);
    this.counters.increment(Bytes.toBytes(counter), 1L);
    metric.count("increments", 1);
  }
}
