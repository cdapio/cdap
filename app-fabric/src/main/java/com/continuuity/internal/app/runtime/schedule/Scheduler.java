package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

import java.util.List;
import java.util.Map;

/**
 * Scheduler that schedules a program.
 */
public interface Scheduler {

  /**
   * Schedule a program to be run in a defined schedule.
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedules Schedule with which the program runs.
   */
  public void schedule(Id.Program program, Type programType, Iterable<Schedule> schedules);

  /**
   * Get the next scheduled run time of the program. A program may contain one or more schedules
   * the method returns the next scheduled runtimes for all the schedules.
   * @param program program to fetch the next runtime.
   * @param programType type of program.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   */
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, Type programType);

  public Map<String, Schedule> getSchedules(Id.Program program, Type programType);

  public void suspendSchedule(String scheduleId);

  public void resumeSchedule(String scheduleId);

}
