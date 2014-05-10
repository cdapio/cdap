package com.continuuity.test;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface WorkflowManager {

  public List<String> getSchedules();

  public ScheduleManager getSchedule(String scheduleName);

  public List<Map<String, String>> getHistory();

}
