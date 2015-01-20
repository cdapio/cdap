/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.proto.Id;

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
   * @param schedule Schedule with which the program runs.
   */
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule);

  /**
   * Schedule a program to be run in a defined schedule.
   *
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedules Schedules with which the program runs.
   */
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules);

  /**
   * Get the next scheduled run time of the program. A program may contain one or more schedules
   * the method returns the next scheduled runtimes for all the schedules.
   *
   * @param program program to fetch the next runtime.
   * @param programType type of program.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   */
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType);

  /**
   * Get Schedule ids for a given program and program type.
   *
   * @param program program for which schedules needs to be determined.
   * @param programType type of program.
   * @return List of scheduleIds, empty List if there are no matching schedules.
   */
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType);

  /**
   * Suspends a schedule. Sub-sequent schedules will not trigger for the job.
   * @param program the program for which schedule needs to be suspended
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   */
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName);

  /**
   * Resume given schedule. The scheduled job will trigger from the next possible runtime.
   * The schedules between pause and resume calls will not be re-run.
   *
   * @param program the program for which schedule needs to be resumed
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   */
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName);

  /**
   * Deletes the schedule.
   * Deletes the associated Job if no other schedules exist for that job.
   *
   * @param scheduleName the name of the schedule
   */
  public void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName);

  /**
   * Delete all schedules associated with the given Program.
   * Also deletes the associated job.
   *
   * @param programId Id of program that needs to be run.
   * @param programType type of program that needs to be run.
   */
  public void deleteSchedules(Id.Program programId, SchedulableProgramType programType);

  /**
   * Get state of a particular schedule.
   *
   * @param program the program for which the state of the schedule is queried
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   * @return State of the schedule.
   */
  public ScheduleState scheduleState (Id.Program program, SchedulableProgramType programType, String scheduleName);

  /**
   * Schedule state.
   */
  public enum ScheduleState { NOT_FOUND, SCHEDULED, SUSPENDED }

}
