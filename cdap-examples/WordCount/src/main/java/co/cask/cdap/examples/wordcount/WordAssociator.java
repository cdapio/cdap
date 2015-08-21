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
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletContext;

import java.util.Set;

/**
 * Word associator Flowlet.
 */
public class WordAssociator extends AbstractFlowlet {

  @Property
  private final String wordAssocTableName;

  private AssociationTable associationTable;

  public WordAssociator(String wordAssocTableName) {
    this.wordAssocTableName = wordAssocTableName;
  }

  @Override
  public void configure(FlowletConfigurer configurer) {
    super.configure(configurer);
    useDatasets(wordAssocTableName);
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    associationTable = context.getDataset(wordAssocTableName);
  }

  @ProcessInput
  public void process(Set<String> words) {
    
    // Store word associations
    this.associationTable.writeWordAssocs(words);
  }
}
