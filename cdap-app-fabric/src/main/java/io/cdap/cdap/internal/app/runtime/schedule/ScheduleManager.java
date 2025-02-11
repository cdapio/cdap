/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.List;

/**
 * Abstract class to manager program schedules.
 */
public abstract class ScheduleManager {

  protected final CConfiguration cConf;
  protected final TransactionRunner transactionRunner;
  protected final AdminEventPublisher adminEventPublisher;

  /**
   * Parameterized constructor for ScheduleManager.
   *
   * @param transactionRunner TransactionRunner
   * @param messagingService MessagingService
   * @param cConf CConfiguration
   */
  public ScheduleManager(TransactionRunner transactionRunner,
      MessagingService messagingService, CConfiguration cConf) {
    this.cConf = cConf;
    this.transactionRunner = transactionRunner;
    MultiThreadMessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    this.adminEventPublisher = new AdminEventPublisher(cConf, messagingContext);
  }

  /**
   * Add program schedules for the given collection of Program Schedules.
   *
   * @param schedules Collection of Program Schedules.
   *
   * @throws BadRequestException if the program type or not workflow.
   * @throws NotFoundException if the program was not found.
   * @throws IOException if there was an internal error in adding schedules.
   * @throws ConflictException if the profile is disabled.
   */
  public abstract void addSchedules(Iterable<? extends ProgramSchedule> schedules)
      throws Exception;

  /**
   * Deleted a program schedule for the given {@code scheduleId}.
   *
   * @param scheduleId for the schedule to be deleted.
   *
   * @throws NotFoundException if the schedule is not found.
   * @throws IOException if an internal error occurred when deleting the schedule.
   */
  public abstract void deleteSchedule(ScheduleId scheduleId)
      throws Exception;

  /**
   * Deletes all the schedules for the given {@code ApplicationId}.
   *
   * @param appId the {@code ApplicationId} whose schedules need to be deleted.
   */
  public void deleteSchedules(ApplicationId appId) {
    TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      List<ProgramSchedule> programSchedules = store.listSchedules(appId);
      for (ProgramSchedule programSchedule : programSchedules) {
        deleteSchedule(programSchedule.getScheduleId());
      }
    }, RuntimeException.class);
  }

  /**
   * Deletes all the schedules for the given {@code ProgramId}.
   *
   * @param programId the {@code ProgramId} whose schedules need to be deleted.
   */
  public void deleteSchedules(ProgramId programId) {
    TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      List<ProgramSchedule> programSchedules = store.listSchedules(programId);
      for (ProgramSchedule programSchedule : programSchedules) {
        deleteSchedule(programSchedule.getScheduleId());
      }
    }, RuntimeException.class);
  }

  /**
   * Update all schedules that can be triggered by the given deleted program. A schedule will be
   * removed if the only {@link ProgramStatusTrigger} in it is triggered by the deleted program.
   * Schedules with composite triggers will be updated if the composite trigger can still be
   * satisfied after the program is deleted, otherwise the schedules will be deleted.
   *
   * @param programId the program id for which to delete the schedules.
   */
  public void modifySchedulesTriggeredByDeletedProgram(ProgramId programId) {
    TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      List<ProgramSchedule> deletedSchedules = store.modifySchedulesTriggeredByDeletedProgram(programId);
      deletedSchedules.forEach(adminEventPublisher::publishScheduleDeletion);
    }, RuntimeException.class);
  }

  /**
   * Lists all the schedules for the given {@code ApplicationId}.
   *
   * @param appId the {@code ApplicationId}.
   *
   * @return List of {@code ProgramSchedule}.
   */
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      return store.listSchedules(appId);
    }, RuntimeException.class);
  }
}
