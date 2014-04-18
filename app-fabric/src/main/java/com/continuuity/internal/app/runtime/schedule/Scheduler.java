package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

import java.util.List;

/**
 * Interfaces that defines all methods related to scheduling, un-scheduling jobs.
 */
public interface Scheduler {

  /**
   * Schedule a program to be run in a defined schedule.
   *
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedules Schedule with which the program runs.
   */
  public void schedule(Id.Program program, Type programType, Iterable<Schedule> schedules);

  /**
   * Get the next scheduled run time of the program. A program may contain one or more schedules
   * the method returns the next scheduled runtimes for all the schedules.
   *
   * @param program program to fetch the next runtime.
   * @param programType type of program.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   */
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, Type programType);

  /**
   * Get Schedule ids for a given program and program type.
   *
   * @param program program for which schedules needs to be determined.
   * @param programType type of program.
   * @return List of scheduleIds, empty List if there are no matching schedules.
   */
  public List<String> getScheduleIds(Id.Program program, Type programType);

  /**
   * Suspends a schedule with the given ScheduleId. Sub-sequent schedules will not be for the job.
   *
   * @param scheduleId schedule id.
   */
  public void suspendSchedule(String scheduleId);

  /**
   * Resume given schedule. The scheduled job will trigger from the next possible runtime.
   * The schedules between pause and resume calls will not be re-run.
   *
   * @param scheduleId schedule id.
   */
  public void resumeSchedule(String scheduleId);

  /**
   * Delete the schedule. Don't schedule any more jobs.
   *
   * @param programId
   * @param programType
   * @param scheduleIds
   */
  public void deleteSchedules(Id.Program programId, Type programType, List<String> scheduleIds);

  /**
   * Get state of a particular schedule.
   *
   * @param scheduleId ScheduleId for getting the state.
   * @return State of the schedule.
   */
  public ScheduleState scheduleState (String scheduleId);

  /**
   * Schedule state.
   */
  public enum ScheduleState { NOT_FOUND, SCHEDULED, SUSPENDED }

}
