/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

import com.google.common.base.Objects;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Used to interact with the Scheduler. Performs authorization checks and other wrapper like functionality.
 *
 * TODO: push predicates down to the lowest level to make things more efficient
 */
public class ProgramScheduleService {
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Scheduler scheduler;
  private final TimeSchedulerService timeSchedulerService;

  @Inject
  ProgramScheduleService(AccessEnforcer accessEnforcer,
                         AuthenticationContext authenticationContext, Scheduler scheduler,
                         TimeSchedulerService timeSchedulerService) {
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.scheduler = scheduler;
    this.timeSchedulerService = timeSchedulerService;
  }

  /**
   * Get the previous run time for the program. A program may contain one or more schedules
   * the method returns the previous runtimes for all the schedules. This method only takes
   + into account schedules based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param programReference program to fetch the previous runtime.
   * @return list of Scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error from the scheduler
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public List<ScheduledRuntime> getPreviousScheduledRuntimes(ProgramReference programReference) throws Exception {
    accessEnforcer.enforce(programReference, authenticationContext.getPrincipal(), StandardPermission.GET);
    return timeSchedulerService.previousScheduledRuntime(programReference);
  }

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
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public List<ScheduledRuntime> getNextScheduledRuntimes(ProgramReference programReference) throws Exception {
    accessEnforcer.enforce(programReference, authenticationContext.getPrincipal(), StandardPermission.GET);
    return timeSchedulerService.nextScheduledRuntime(programReference);
  }

  /**
   * Add the given schedule
   *
   * @param schedule the schedule to add
   * @throws AlreadyExistsException if one of the schedules already exists
   * @throws NotFoundException if there is a profile assigned to the schedule and it does not exist
   * @throws ProfileConflictException if there is a profile assigned to the schedule and it is diabled
   * @throws BadRequestException if the schedule is invalid
   * @throws UnauthorizedException if the principal is not authorized as an admin operations on the schedule app
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public void add(ProgramSchedule schedule) throws Exception {
    accessEnforcer.enforce(schedule.getProgramReference().getParent(),
                           authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    scheduler.addSchedule(schedule);
  }

  /**
   * Update the given schedule
   *
   * @param scheduleId the schedule to update
   * @param scheduleDetail the schedule to update it to
   * @throws AlreadyExistsException if one of the schedules already exists
   * @throws NotFoundException if there is a profile assigned to the schedule and it does not exist
   * @throws ProfileConflictException if there is a profile assigned to the schedule and it is diabled
   * @throws BadRequestException if the update is invalid
   * @throws UnauthorizedException if the principal is not authorized as an admin operations on the schedule program
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public void update(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    accessEnforcer.enforce(scheduleId.getParent(), authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    ProgramSchedule existing = scheduler.getSchedule(scheduleId);

    String description = Objects.firstNonNull(scheduleDetail.getDescription(), existing.getDescription());
    ProgramReference programReference = scheduleDetail.getProgram() == null ? existing.getProgramReference()
      : existing.getProgramReference().getParent().program(
      scheduleDetail.getProgram().getProgramType() == null ? existing.getProgramReference().getType()
        : ProgramType.valueOfSchedulableType(scheduleDetail.getProgram().getProgramType()),
      Objects.firstNonNull(scheduleDetail.getProgram().getProgramName(), existing.getProgramReference().getProgram()));
    if (!programReference.equals(existing.getProgramReference())) {
      throw new BadRequestException(
        String.format("Must update the schedule '%s' with the same program as '%s'. "
                        + "To change the program in a schedule, please delete the schedule and create a new one.",
                      existing.getName(), existing.getProgramReference().toString()));
    }
    Map<String, String> properties = Objects.firstNonNull(scheduleDetail.getProperties(), existing.getProperties());
    Trigger trigger = Objects.firstNonNull(scheduleDetail.getTrigger(), existing.getTrigger());
    List<? extends Constraint> constraints =
      Objects.firstNonNull(scheduleDetail.getConstraints(), existing.getConstraints());
    Long timeoutMillis = Objects.firstNonNull(scheduleDetail.getTimeoutMillis(), existing.getTimeoutMillis());
    ProgramSchedule updatedSchedule = new ProgramSchedule(existing.getName(), description, programReference, properties,
                                                          trigger, constraints, timeoutMillis);
    scheduler.updateSchedule(updatedSchedule);
  }

  /**
   * Get the given schedule
   *
   * @return the schedule
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to access the schedule program
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public ProgramSchedule get(ScheduleId scheduleId) throws Exception {
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    accessEnforcer.enforce(schedule.getProgramReference(), authenticationContext.getPrincipal(),
                           StandardPermission.GET);
    return schedule;
  }

  /**
   * Get the schedule record for the given schedule ID
   *
   * @return the schedule
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to access the schedule program
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public ProgramScheduleRecord getRecord(ScheduleId scheduleId) throws Exception {
    ProgramScheduleRecord record = scheduler.getScheduleRecord(scheduleId);
    accessEnforcer.enforce(record.getSchedule().getProgramReference(), authenticationContext.getPrincipal(),
                           StandardPermission.GET);
    return record;
  }

  /**
   * Get the state of the given schedule
   *
   * @return the status of the given schedule
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to access the schedule program
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public ProgramScheduleStatus getStatus(ScheduleId scheduleId) throws Exception {
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    accessEnforcer.enforce(schedule.getProgramReference(), authenticationContext.getPrincipal(),
                           StandardPermission.GET);
    return scheduler.getScheduleStatus(scheduleId);
  }

  /**
   * List the schedules for the given app that match the given predicate
   *
   * @param appReference the application to get schedules for
   * @param predicate return schedules that match this predicate
   * @return schedules for the given app that match the given predicate
   * @throws NotFoundException if the application could not be found
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> list(ApplicationReference appReference,
                                                Predicate<ProgramScheduleRecord> predicate) throws Exception {
    accessEnforcer.enforce(appReference, authenticationContext.getPrincipal(), StandardPermission.GET);
    return scheduler.listScheduleRecords(appReference).stream().filter(predicate)
      .collect(Collectors.toList());
  }

  /**
   * List the schedules for the given program that match the given predicate
   *
   * @param programReference the program to get schedules for
   * @param predicate return schedules that match this predicate
   * @return schedules for the given program that match the given predicate
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> list(ProgramReference programReference,
                                                Predicate<ProgramScheduleRecord> predicate) throws Exception {
    accessEnforcer.enforce(programReference, authenticationContext.getPrincipal(), StandardPermission.GET);
    return scheduler.listScheduleRecords(programReference).stream().filter(predicate).collect(Collectors.toList());
  }

  /**
   * Get schedules that are triggered by the given program statuses.
   *
   * @param programReference the program that is in the trigger
   * @param statusSet statuses that are involved in the trigger
   * @return schedules triggered by the given program statuses
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> findTriggeredBy(ProgramReference programReference,
                                                           Set<ProgramStatus> statusSet) throws Exception {
    accessEnforcer.enforce(programReference, authenticationContext.getPrincipal(), StandardPermission.GET);

    Set<ProgramScheduleRecord> schedules = new HashSet<>();
    scheduler.findSchedules(programReference.toString());
    for (String triggerKey : Schedulers.triggerKeysForProgramStatuses(programReference, statusSet)) {
      schedules.addAll(scheduler.findSchedules(triggerKey));
    }
    return schedules;
  }

  /**
   * Suspend the given schedule
   *
   * @param scheduleId the schedule to suspend
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to suspend the schedule
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public void suspend(ScheduleId scheduleId) throws Exception {
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    accessEnforcer.enforce(schedule.getProgramReference(), authenticationContext.getPrincipal(),
                           ApplicationPermission.EXECUTE);
    scheduler.disableSchedule(scheduleId);
  }

  /**
   * Resume the given schedule
   *
   * @param scheduleId the schedule to suspend
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to suspend the schedule
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public void resume(ScheduleId scheduleId) throws Exception {
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    accessEnforcer.enforce(schedule.getProgramReference(), authenticationContext.getPrincipal(),
                           ApplicationPermission.EXECUTE);
    scheduler.enableSchedule(scheduleId);
  }

  /**
   * Delete the given schedule
   *
   * @param scheduleId the schedule to delete
   * @throws NotFoundException if the schedule could not be found
   * @throws UnauthorizedException if the principal is not authorized to delete the schedule
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public void delete(ScheduleId scheduleId) throws Exception {
    accessEnforcer.enforce(scheduleId.getParent(), authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    scheduler.deleteSchedule(scheduleId);
  }

  /**
   * Enables all schedules which were disabled or added between startTimeMillis and endTimeMillis in a given namespace.
   *
   * @param namespaceId the namespace to re-enable schedules in
   * @param startTimeMillis the lower bound in millis for when the schedule was disabled (inclusive)
   * @param endTimeMillis the upper bound in millis for when the schedule was disabled (exclusive)
   * @throws ConflictException if the schedule was already enabled
   */
  public void reEnableSchedules(NamespaceId namespaceId, long startTimeMillis, long endTimeMillis) throws Exception {
    accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    scheduler.reEnableSchedules(namespaceId, startTimeMillis, endTimeMillis);
  }
}
