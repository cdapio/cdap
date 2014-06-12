package com.continuuity.test;

import com.continuuity.app.program.RunRecord;

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
