/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.scheduler;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;

import java.util.Collection;
import java.util.List;

/**
 * Scheduler interface for deploying and retrieving schedules.
 */
public interface Scheduler {

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @throws AlreadyExistsException if the schedule already exists
   */
  void addSchedule(ProgramSchedule schedule) throws AlreadyExistsException, BadRequestException;

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   * @throws AlreadyExistsException if one of the schedules already exists
   */
  void addSchedules(Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException, BadRequestException;

  /**
   * Updates a schedule in the store. The schedule with the same {@link ScheduleId}
   * as the given {@code schedule} will be replaced.
   *
   * @param schedule the new schedule. The existing schedule with the same {@link ScheduleId} will be replaced
   * @throws NotFoundException if the schedule with {@link ScheduleId} does not exist in the store
   */
  void updateSchedule(ProgramSchedule schedule) throws NotFoundException, BadRequestException;

  /**
   * Enables a schedule. The schedule must be currently disabled.
   *
   * @param scheduleId the schedule to enable
   * @throws NotFoundException if the schedule does not exist in the store
   * @throws ConflictException if the schedule was already enabled
   */
  void enableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException;

  /**
   * Disable a schedule. The schedule must be currently enabled.
   *
   * @param scheduleId the schedule to disable
   * @throws NotFoundException if the schedule does not exist in the store
   * @throws ConflictException if the schedule was already disabled
   */
  void disableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException;

  /**
   * Removes a schedule from the store. Succeeds whether the schedule exists or not.
   *
   * @param scheduleId the schedule to delete
   * @throws NotFoundException if the schedule does not exist in the store
   */
  void deleteSchedule(ScheduleId scheduleId) throws NotFoundException;

  /**
   * Removes one or more schedules from the store. Succeeds whether the schedules exist or not.
   *
   * @param scheduleIds the schedules to delete
   * @throws NotFoundException if one of the schedules does not exist in the store
   */
  void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException;

  /**
   * Removes all schedules for a specific application from the store.
   *
   * @param appId the application id for which to delete the schedules
   */
  void deleteSchedules(ApplicationId appId);

  /**
   * Removes all schedules for a specific program from the store.
   *
   * @param programId the program id for which to delete the schedules
   */
  void deleteSchedules(ProgramId programId);

  /**
   * Update all schedules that can be triggered by the given deleted program. Schedules will be removed if they
   * contain single {@link co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger}. Schedules with
   * composite triggers will be updated if the composite trigger can still be satisfied after the program is deleted,
   * otherwise the schedules will be deleted.
   *
   * @param programId id of the deleted program
   */
  void modifySchedulesTriggeredByDeletedProgram(ProgramId programId);

  /**
   * Read a schedule from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the schedule from the store
   * @throws NotFoundException if the schedule does not exist in the store
   */
  ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException;

  /**
   * Read a schedule's status from the store.
   *
   * @param scheduleId the id of the schedule to read
   * @return the status of the schedule
   * @throws NotFoundException if the schedule does not exist in the store
   */
  ProgramScheduleStatus getScheduleStatus(ScheduleId scheduleId) throws NotFoundException;

  /**
   * Retrieve all schedules for a given application.
   *
   * @param appId the application for which to list the schedules.
   * @return a list of schedules for the application; never null
   */
  List<ProgramSchedule> listSchedules(ApplicationId appId) throws NotFoundException;

  /**
   * Retrieve all schedules for a given program.
   *
   * @param programId the program for which to list the schedules.
   * @return a list of schedules for the program; never null
   */
  List<ProgramSchedule> listSchedules(ProgramId programId) throws NotFoundException;

  /**
   * Retrieve all schedule records for a given application.
   *
   * @param appId the application for which to list the schedule records.
   * @return a list of schedule records for the application; never null
   */
  List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId) throws NotFoundException;

  /**
   * Retrieve all schedule records for a given program.
   *
   * @param programId the program for which to list the schedule records.
   * @return a list of schedule records for the program; never null
   */
  List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId) throws NotFoundException;

  /**
   * Find all schedules for a given trigger key
   */
  Collection<ProgramScheduleRecord> findSchedules(String triggerKey);
}
