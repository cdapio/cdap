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

package co.cask.cdap.test;

import co.cask.cdap.proto.RunRecord;

import java.util.List;

/**
 * Workflow manager interface for managing the workflow and its schedules
 */
public interface WorkflowManager {

  /**
   * Get the list of schedules of the workflow
   * @return List of Schedule Ids.
   */
  public List<String> getSchedules();

  /**
   * Get the {@link ScheduleManager} instance to manage the schedule
   * @param scheduleId of the workflow to retrieve
   * @return {@link ScheduleManager} instance to manage the schedule identified by scheduleId
   */
  public ScheduleManager getSchedule(String scheduleId);

  /**
   * Get the history of the workflow
   * @return list of {@link RunRecord} workflow history
   */
  public List<RunRecord> getHistory();
}
