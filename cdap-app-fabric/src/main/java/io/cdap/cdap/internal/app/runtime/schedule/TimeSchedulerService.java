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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;

import java.util.List;

/**
 * TimeSchedulerService interface to define start/stop and scheduling of jobs.
 */
public interface TimeSchedulerService extends Service {
  /**
   * Add a new schedule converted from the given {@link ProgramSchedule}.
   *
   * @param schedule the {@link ProgramSchedule} to convert and add
   * @throws AlreadyExistsException if the schedule already exists
   * @throws SchedulerException on unforeseen error
   */
  void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException, SchedulerException;

  /**
   * Deletes the schedule with the same schedule name and {@link ProgramId} as the given {@link ProgramSchedule}.
   * If the schedule does not exist, this is a no-op.
   *
   * @param schedule the {@link ProgramSchedule} with schedule name and {@link ProgramId} of the schedule to delete
   * @throws SchedulerException on unforeseen error
   */
  void deleteProgramSchedule(ProgramSchedule schedule) throws SchedulerException;

  /**
   * Suspends the schedule with the same schedule name and {@link ProgramId} as the given {@link ProgramSchedule}.
   * Sub-sequent schedules will not trigger for the job. If the schedule is already suspended,
   * this method will not do anything.
   *
   * @param schedule the {@link ProgramSchedule} with schedule name and {@link ProgramId} of the schedule to suspend
   * @throws NotFoundException if the schedule could not be found
   * @throws SchedulerException on unforeseen error
   */
  void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException;

  /**
   * Resume the schedule with the same schedule name and {@link ProgramId} as the given {@link ProgramSchedule}.
   * If the schedule is already active, this method will not do anything.
   * The time schedules between suspend and resume calls will not be re-run - the scheduled job will trigger
   * from the next possible runtime. For data schedules based on size, the execution will be triggered only once,
   * even if the data requirement has been met multiple times.
   *
   * @param schedule the {@link ProgramSchedule} with schedule name and {@link ProgramId} of the schedule to suspend
   * @throws NotFoundException if the schedule could not be found
   * @throws SchedulerException on unforeseen error.
   */
  void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException;

  /**
   * Get the previous run time for the program. A program may contain one or more schedules
   * the method returns the previous runtimes for all the schedules. This method only takes
   + into account schedules based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param programReference program to fetch the previous runtime.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  List<ScheduledRuntime> previousScheduledRuntime(ProgramReference programReference) throws SchedulerException;

  /**
   * Get the next scheduled run time of the program. A program may contain multiple schedules.
   * This method returns the next scheduled runtimes for all the schedules. This method only takes
   + into account schedules based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param programReference program to fetch the next runtime.
   * @return list of scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  List<ScheduledRuntime> nextScheduledRuntime(ProgramReference programReference) throws SchedulerException;

  /**
   * Get all the scheduled run time of the program within the given time range in the future.
   * A program may contain multiple schedules. This method returns the scheduled runtimes for all the schedules
   * within the given time range. This method only takes
   + into account schedules based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param programReference program to fetch the next runtime.
   * @param programType type of program.
   * @param startTimeSecs the start of the time range in seconds (inclusive, i.e. scheduled time larger or
   *                      equal to the start will be returned)
   * @param endTimeSecs the end of the time range in seconds (exclusive, i.e. scheduled time smaller than the end
   *                    will be returned)
   * @return list of scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  List<ScheduledRuntime> getAllScheduledRunTimes(ProgramReference programReference, SchedulableProgramType programType,
                                                 long startTimeSecs, long endTimeSecs) throws SchedulerException;
}
