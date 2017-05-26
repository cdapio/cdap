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

/**
 * App with a schedule triggered by new partition in the dataset with name "rawRecords"
 */
public class AppWithDataPartitionSchedule extends AbstractApplication {
  public static final String SOME_WORKFLOW = "SomeWorkflow";
  public static final String DATASET_PARTITION_SCHEDULE_1 = "DataSetPartionSchedule1";
  public static final int TRIGGER_ON_NUM_PARTITIONS = 5;


  @Override
  public void configure() {
    setName("AppWithDataPartitionSchedule");
    setDescription("Sample application with data partition triggered schedule");
    addWorkflow(new AppWithFrequentScheduledWorkflows.DummyWorkflow(SOME_WORKFLOW));
    configureWorkflowSchedule(DATASET_PARTITION_SCHEDULE_1, SOME_WORKFLOW)
      .triggerOnPartitions("rawRecords", TRIGGER_ON_NUM_PARTITIONS);
  }
}
