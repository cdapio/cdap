/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Objects;
import com.google.inject.Inject;

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
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Scheduler scheduler;

  @Inject
  ProgramScheduleService(AuthorizationEnforcer authorizationEnforcer,
                         AuthenticationContext authenticationContext, Scheduler scheduler) {
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.scheduler = scheduler;
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
    authorizationEnforcer.enforce(schedule.getProgramId().getParent(),
                                  authenticationContext.getPrincipal(), Action.ADMIN);
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
    authorizationEnforcer.enforce(scheduleId.getParent(), authenticationContext.getPrincipal(), Action.ADMIN);
    ProgramSchedule existing = scheduler.getSchedule(scheduleId);

    String description = Objects.firstNonNull(scheduleDetail.getDescription(), existing.getDescription());
    ProgramId programId = scheduleDetail.getProgram() == null ? existing.getProgramId()
      : existing.getProgramId().getParent().program(
      scheduleDetail.getProgram().getProgramType() == null ? existing.getProgramId().getType()
        : ProgramType.valueOfSchedulableType(scheduleDetail.getProgram().getProgramType()),
      Objects.firstNonNull(scheduleDetail.getProgram().getProgramName(), existing.getProgramId().getProgram()));
    if (!programId.equals(existing.getProgramId())) {
      throw new BadRequestException(
        String.format("Must update the schedule '%s' with the same program as '%s'. "
                        + "To change the program in a schedule, please delete the schedule and create a new one.",
                      existing.getName(), existing.getProgramId().toString()));
    }
    Map<String, String> properties = Objects.firstNonNull(scheduleDetail.getProperties(), existing.getProperties());
    Trigger trigger = Objects.firstNonNull(scheduleDetail.getTrigger(), existing.getTrigger());
    List<? extends Constraint> constraints =
      Objects.firstNonNull(scheduleDetail.getConstraints(), existing.getConstraints());
    Long timeoutMillis = Objects.firstNonNull(scheduleDetail.getTimeoutMillis(), existing.getTimeoutMillis());
    ProgramSchedule updatedSchedule = new ProgramSchedule(existing.getName(), description, programId, properties,
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
    AuthorizationUtil.ensureAccess(schedule.getProgramId(), authorizationEnforcer,
                                   authenticationContext.getPrincipal());
    return schedule;
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
    AuthorizationUtil.ensureAccess(schedule.getProgramId(), authorizationEnforcer,
                                   authenticationContext.getPrincipal());
    return scheduler.getScheduleStatus(scheduleId);
  }

  /**
   * List the schedules for the given app that match the given predicate
   *
   * @param applicationId the application to get schedules for
   * @param predicate return schedules that match this predicate
   * @return schedules for the given app that match the given predicate
   * @throws NotFoundException if the application could not be found
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> list(ApplicationId applicationId,
                                                Predicate<ProgramScheduleRecord> predicate) throws Exception {
    AuthorizationUtil.ensureAccess(applicationId, authorizationEnforcer, authenticationContext.getPrincipal());
    return scheduler.listScheduleRecords(applicationId).stream().filter(predicate).collect(Collectors.toList());
  }

  /**
   * List the schedules for the given workflow that match the given predicate
   *
   * @param workflowId the workflow to get schedules for
   * @param predicate return schedules that match this predicate
   * @return schedules for the given program that match the given predicate
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> list(WorkflowId workflowId,
                                                Predicate<ProgramScheduleRecord> predicate) throws Exception {
    AuthorizationUtil.ensureAccess(workflowId, authorizationEnforcer, authenticationContext.getPrincipal());
    return scheduler.listScheduleRecords(workflowId).stream().filter(predicate).collect(Collectors.toList());
  }

  /**
   * Get schedules that are triggered by the given program statuses.
   *
   * @param programId the program that is in the trigger
   * @param statusSet statuses that are involved in the trigger
   * @return schedules triggered by the given program statuses
   * @throws UnauthorizedException if the principal is not authorized to access the application
   * @throws Exception if any other errors occurred while performing the authorization enforcement check
   */
  public Collection<ProgramScheduleRecord> findTriggeredBy(ProgramId programId,
                                                           Set<ProgramStatus> statusSet) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());

    Set<ProgramScheduleRecord> schedules = new HashSet<>();
    scheduler.findSchedules(programId.toString());
    for (String triggerKey : Schedulers.triggerKeysForProgramStatuses(programId, statusSet)) {
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
    authorizationEnforcer.enforce(schedule.getProgramId(), authenticationContext.getPrincipal(), Action.EXECUTE);
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
    authorizationEnforcer.enforce(schedule.getProgramId(), authenticationContext.getPrincipal(), Action.EXECUTE);
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
    authorizationEnforcer.enforce(scheduleId.getParent(), authenticationContext.getPrincipal(), Action.ADMIN);
    scheduler.deleteSchedule(scheduleId);
  }
}
