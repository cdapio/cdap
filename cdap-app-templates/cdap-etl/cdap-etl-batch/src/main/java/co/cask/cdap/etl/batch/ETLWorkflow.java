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
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.batch.spark.ETLSpark;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;

/**
 * Workflow for scheduling Batch ETL MapReduce Driver.
 */
public class ETLWorkflow extends AbstractWorkflow {

  public static final String NAME = "ETLWorkflow";
  public static final String DESCRIPTION = "Workflow for ETL Batch MapReduce Driver";

  private final ETLBatchConfig config;

  public ETLWorkflow(ETLBatchConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription(DESCRIPTION);
    switch (config.getEngine()) {
      case MAPREDUCE:
        addMapReduce(ETLMapReduce.NAME);
        break;
      case SPARK:
        addSpark(ETLSpark.class.getSimpleName());
        break;
    }
    if (config.getActions() != null) {
      for (ETLStage action : config.getActions()) {
        if (!action.getPlugin().getName().equals("Email")) {
          throw new IllegalArgumentException(String.format("Only \'Email\' actions are supported. " +
                                                             "You cannot create an action of type %s.",
                                                           action.getPlugin().getName()));
        }
        addAction(new EmailAction(action));
      }
    }
  }
}
