package com.continuuity.test;

import java.util.List;
import java.util.Map;

/**
 * Workflow manager interface for managing the workflow and its schedules.
 */
public interface WorkflowManager {

  /**
   * Get the list of schedules of the workflow
   * @return
   */
  public List<String> getSchedules();

  /**
   * Get the {@link ScheduleManager} instance to manage the schedule
   * @param scheduleName
   * @return
   */
  public ScheduleManager getSchedule(String scheduleName);

  /**
   * Get the history of the workflow
   * @return
   */
  public List<Map<String, String>> getHistory();
}
