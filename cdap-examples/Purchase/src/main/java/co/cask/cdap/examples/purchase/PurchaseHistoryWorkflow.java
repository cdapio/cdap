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
package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;

/**
 * Implements a simple Workflow with one Workflow action to run the PurchaseHistoryBuilder 
 * MapReduce with a schedule that runs every day at 4:00 A.M.
 */
public class PurchaseHistoryWorkflow implements Workflow {

  @Override
  public WorkflowSpecification configure() {
    return WorkflowSpecification.Builder.with()
      .setName("PurchaseHistoryWorkflow")
      .setDescription("PurchaseHistoryWorkflow description")
      .onlyWith(new PurchaseHistoryBuilder())
      .addSchedule(new Schedule("DailySchedule", "Run every day at 4:00 A.M.", "0 4 * * *",
                                       Schedule.Action.START))
      .build();
  }
}
