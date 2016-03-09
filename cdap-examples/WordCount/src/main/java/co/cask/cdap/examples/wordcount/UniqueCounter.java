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
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;

/**
 * Flowlet that updates unique count of words.
 */
public class UniqueCounter extends AbstractFlowlet {

  @Property
  private final String uniqueCountTableName;

  private UniqueCountTable uniqueCountTable;

  public UniqueCounter(String uniqueCountTableName) {
    this.uniqueCountTableName = uniqueCountTableName;
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    uniqueCountTable = context.getDataset(uniqueCountTableName);
  }

  @ProcessInput
  public void process(String word) {
    this.uniqueCountTable.updateUniqueCount(word);
  }
}
