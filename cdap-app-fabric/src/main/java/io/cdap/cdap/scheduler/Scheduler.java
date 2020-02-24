/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.scheduler;

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Scheduler interface for deploying and retrieving schedules.
 */
public interface Scheduler {

  /**
   * Add a schedule to the store.
   *
   * @param schedule the schedule to add
   * @throws AlreadyExistsException if the schedule already exists
   * @throws NotFoundException if there is a profile assigned to the schedule and it does not exist
   * @throws ProfileConflictException if there is a profile assigned to the schedule and it is diabled
   */
  void addSchedule(ProgramSchedule schedule)
    throws ProfileConflictException, BadRequestException, NotFoundException, AlreadyExistsException;

  /**
   * Add one or more schedules to the store.
   *
   * @param schedules the schedules to add
   * @throws AlreadyExistsException if one of the schedules already exists
   * @throws NotFoundException if there is a profile assigned to the schedule and it does not exist
   * @throws ProfileConflictException if there is a profile assigned to the schedule and it is diabled
   */
  void addSchedules(Iterable<? extends ProgramSchedule> schedules)
    throws AlreadyExistsException, BadRequestException, NotFoundException, ProfileConflictException;

  /**
   * Updates a schedule in the store. The schedule with the same {@link ScheduleId}
   * as the given {@code schedule} will be replaced.
   *
   * @param schedule the new schedule. The existing schedule with the same {@link ScheduleId} will be replaced
   * @throws NotFoundException if the schedule with {@link ScheduleId} does not exist in the store or
   *                           if there is a profile assigned to the schedule and it does not exist
   * @throws ProfileConflictException if there is a profile assigned to the schedule and it is diabled
   */
  void updateSchedule(ProgramSchedule schedule) throws NotFoundException, BadRequestException, ProfileConflictException;

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
   * contain single {@link io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger}. Schedules with
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
   * Get the metadata of the given schedule from the store
   *
   * @param scheduleId the id of the schedule
   * @return the metadata of the schedule from the store
   * @throws NotFoundException if the schedule doesn't exist in the store
   */
  ProgramScheduleMeta getScheduleMetadata(ScheduleId scheduleId) throws NotFoundException;

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
   * Retrieve all schedules for a given namespace
   *
   * @param namespaceId the namespace for which to list the schedules
   * @param filter the filter to be applied on the result schedules
   * @return a list of schedule records for the namespace; never null
   */
  List<ProgramSchedule> listSchedules(NamespaceId namespaceId, Predicate<ProgramSchedule> filter);

  /**
   * Retrieve all schedule records for a given application.
   *
   * @param appId the application for which to list the schedule records.
   * @return a list of schedule records for the application; never null
   */
  List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId);

  /**
   * Retrieve all schedule records for a given program.
   *
   * @param programId the program for which to list the schedule records.
   * @return a list of schedule records for the program; never null
   */
  List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId);

  /**
   * Find all schedules for a given trigger key
   */
  Collection<ProgramScheduleRecord> findSchedules(String triggerKey);

  /**
   * Enables all schedules which were disabled or added between startTimeMillis and endTimeMillis in a given namespace.
   *
   * @param namespaceId the namespace to re-enable schedules in
   * @param startTimeMillis the lower bound in millis for when the schedule was disabled (inclusive)
   * @param endTimeMillis the upper bound in millis for when the schedule was disabled (exclusive)
   * @throws ConflictException if the schedule was already enabled
   */
  void reEnableSchedules(NamespaceId namespaceId, long startTimeMillis, long endTimeMillis) throws ConflictException;
}
