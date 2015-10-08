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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;

/**
 * ETL Batch Application.
 */
public class ETLBatchApplication extends AbstractApplication<ETLBatchConfig> {
  public static final String SCHEDULE_NAME = "etlWorkflow";
  public static final String DEFAULT_DESCRIPTION = "Extract-Transform-Load (ETL) Batch Application";

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig();
    setDescription(DEFAULT_DESCRIPTION);
    addMapReduce(new ETLMapReduce(config));
    addWorkflow(new ETLWorkflow(config));
    scheduleWorkflow(Schedules.createTimeSchedule(SCHEDULE_NAME, "ETL Batch schedule", config.getSchedule()),
                     ETLWorkflow.NAME);
  }
}
