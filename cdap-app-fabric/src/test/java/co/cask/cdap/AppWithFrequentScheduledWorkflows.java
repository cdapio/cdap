/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;

public class AppWithFrequentScheduledWorkflows extends AbstractApplication {
  public static final String SOME_WORKFLOW = "SomeWorkflow";
  public static final String ANOTHER_WORKFLOW = "AnotherWorkflow";
  public static final String DATASET_PARTITION_SCHEDULE_1 = "DataSetPartionSchedule1";
  public static final String DATASET_PARTITION_SCHEDULE_2 = "DataSetPartionSchedule2";
  public static final String DATASET_NAME1 = "SomeDataset";
  public static final String DATASET_NAME2 = "AnotherDataset";
  public static final String ONE_MIN_SCHEDULE_1 = "OneMinSchedule1";
  public static final String ONE_MIN_SCHEDULE_2 = "OneMinSchedule2";
  public static final String SCHEDULED_WORKFLOW_1 = "ScheduledWorkflow1";
  public static final String SCHEDULED_WORKFLOW_2 = "ScheduledWorkflow2";


  @Override
  public void configure() {
    setName("AppWithMultipleWorkflows");
    setDescription("Sample application with multiple Workflows");
    addWorkflow(new DummyWorkflow(SOME_WORKFLOW));
    addWorkflow(new DummyWorkflow(ANOTHER_WORKFLOW));
    addWorkflow(new DummyWorkflow(SCHEDULED_WORKFLOW_1));
    addWorkflow(new DummyWorkflow(SCHEDULED_WORKFLOW_2));
    configureWorkflowSchedule(DATASET_PARTITION_SCHEDULE_1, SOME_WORKFLOW).triggerOnPartitions(DATASET_NAME1, 1);
    configureWorkflowSchedule(DATASET_PARTITION_SCHEDULE_2, ANOTHER_WORKFLOW).triggerOnPartitions(DATASET_NAME2, 1);
    // Schedule the workflow to run in every min
    scheduleWorkflow(Schedules.builder(ONE_MIN_SCHEDULE_1).createTimeSchedule("* * * * *"), SCHEDULED_WORKFLOW_1);
    // Schedule the workflow to run in every min with a different cron expression
    scheduleWorkflow(Schedules.builder(ONE_MIN_SCHEDULE_2).createTimeSchedule("*/1 * * * *"), SCHEDULED_WORKFLOW_2);
  }

  /**
   * Some Workflow
   */
  public static class DummyWorkflow extends AbstractWorkflow {
    final String name;

    public DummyWorkflow(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription("SampleWorkflow description");
      addAction(new AppWithMultipleWorkflows.SomeDummyAction());
    }
  }
}
