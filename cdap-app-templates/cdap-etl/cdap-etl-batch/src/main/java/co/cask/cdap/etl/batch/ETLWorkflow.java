/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;

/**
 * Workflow for scheduling Batch ETL MapReduce Driver.
 */
public class ETLWorkflow extends AbstractWorkflow {

  public static final String NAME = "ETLWorkflow";

  private final ETLBatchConfig config;

  public ETLWorkflow(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Workflow for Batch ETL MapReduce Driver");
    addMapReduce(ETLMapReduce.NAME);
    if (config.getActions() != null) {
      for (ETLStage action : config.getActions()) {
        if (!action.getName().equals("Email")) {
          throw new IllegalArgumentException(String.format("Only \'Email\' actions are supported. " +
                                                             "You cannot create an action of type %s.",
                                                           action.getName()));
        }
        addAction(new EmailAction(action));
      }
    }
  }
}
