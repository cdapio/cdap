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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ScheduledRuntime;

import java.util.List;
import java.util.Map;

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
   * @throws SchedulerException on unforeseen error.
   */
  void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException;

  /**
   * Schedule a program to be run in a defined schedule.
   *
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedule Schedule with which the program runs.
   * @param properties system properties to be passed to the schedule
   * @throws SchedulerException on unforeseen error.
   */
  void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                Map<String, String> properties) throws SchedulerException;

  /**
   * Schedule a program to be run in a defined schedule.
   *
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedules Schedules with which the program runs.
   * @throws SchedulerException on unforeseen error.
   */
  void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException;

  /**
   * Schedule a program to be run in a defined schedule.
   *
   * @param program Program that needs to be run.
   * @param programType type of program.
   * @param schedules Schedules with which the program runs.
   * @param properties system properties to be passed to the schedule.
   * @throws SchedulerException on unforeseen error.
   */
  void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules,
                Map<String, String> properties)
    throws SchedulerException;

  /**
   * Get the previous run time for the program. A program may contain one or more schedules
   * the method returns the previous runtimes for all the schedules. This method only takes
   + into account {@link Schedule}s based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param program program to fetch the previous runtime.
   * @param programType type of program.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  List<ScheduledRuntime> previousScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException;

  /**
   * Get the next scheduled run time of the program. A program may contain one or more schedules
   * the method returns the next scheduled runtimes for all the schedules. This method only takes
   + into account {@link Schedule}s based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param program program to fetch the next runtime.
   * @param programType type of program.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException;

  /**
   * Get Schedule ids for a given program and program type.
   *
   * @param program program for which schedules needs to be determined.
   * @param programType type of program.
   * @return List of scheduleIds, empty List if there are no matching schedules.
   * @throws SchedulerException on unforeseen error.
   */
  List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException;

  /**
   * Suspends a schedule. Sub-sequent schedules will not trigger for the job. If the schedule is already suspended,
   * this method will not do anything.
   *
   * @param program the program for which schedule needs to be suspended
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   * @throws NotFoundException if the {@code scheduleName} could not be found, or if the application the {@code program}
   *                           belongs to does not exist.
   * @throws SchedulerException on unforeseen error.
   */
  void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException;

  /**
   * Resume given schedule. If the schedule is already active, this method will not do anything.
   * The time schedules between suspend and resume calls will not be re-run - the scheduled job will trigger
   * from the next possible runtime. For data schedules based on size, the execution will be triggered only once,
   * even if the data requirement has been met multiple times.
   *
   * @param program the program for which schedule needs to be resumed
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   * @throws NotFoundException if the {@code scheduleName} could not be found, or if the application the {@code program}
   *                           belongs to does not exist.
   * @throws SchedulerException on unforeseen error.
   */
  void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException;

  /**
   * Update the given schedule. The schedule with the same name than the given {@code schedule} will be replaced.
   *
   * @param program the program for which schedule needs to be updated
   * @param programType the type of the program
   * @param schedule the new schedule. The schedule with the same name will be replaced
   * @throws NotFoundException if the {@code schedule} does not exist, or if the application the {@code program}
   *                           belongs to does not exist.
   * @throws SchedulerException on unforeseen error.
   */
  void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException;

  /**
   * Update the given schedule. The schedule with the same name than the given {@code schedule} will be replaced.
   *
   * @param program the program for which schedule needs to be updated
   * @param programType the type of the program
   * @param schedule the new schedule. The schedule with the same name will be replaced
   * @param properties properties that can be passed to the quartz scheduler
   * @throws NotFoundException if the {@code schedule} does not exist, or if the application the {@code program}
   *                           belongs to does not exist.
   * @throws SchedulerException on unforeseen error.
   */
  void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException;

  /**
   * Deletes the schedule.
   * Deletes the associated Job if no other schedules exist for that job.
   *
   * @param scheduleName the name of the schedule
   * @throws NotFoundException if the {@code scheduleName} could not be found, or if the application the {@code program}
   *                           belongs to does not exist.
   * @throws SchedulerException on unforeseen error.
   */
  void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException;

  /**
   * Delete all schedules associated with the given Program.
   * Also deletes the associated job.
   *
   * @param programId Id of program that needs to be run.
   * @param programType type of program that needs to be run.
   * @throws SchedulerException on unforeseen error.
   */
  void deleteSchedules(Id.Program programId, SchedulableProgramType programType)
    throws SchedulerException;

  /**
   * Deletes all schedules associated with the given namespace.
   *
   * @param namespaceId Id of the namespace
   * @throws SchedulerException on unforeseen error
   */
  void deleteAllSchedules(Id.Namespace namespaceId) throws SchedulerException;

  /**
   * Get state of a particular schedule.
   *
   * @param program the program for which the state of the schedule is queried
   * @param programType the type of the program
   * @param scheduleName the name of the schedule
   * @return State of the schedule.
   * @throws SchedulerException on unforeseen error.
   */
  ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException;

  /**
   * Schedule state.
   */
  enum ScheduleState { NOT_FOUND, SCHEDULED, SUSPENDED }

}
