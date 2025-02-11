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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueueTable;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.store.profile.ProfileStore;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ScheduleManager} to manage program schedules. This class is meant to be used by in-memory
 * modules and tests.
 */
public class LocalScheduleManager extends ScheduleManager {

  private static final Logger LOG = LoggerFactory.getLogger(LocalScheduleManager.class);
  private final TimeSchedulerService timeSchedulerService;

  /**
   * Parameterized constructor for LocalScheduleManager.
   *
   * @param transactionRunner TransactionRunner
   * @param messagingService MessagingService
   * @param cConf CConfiguration
   * @param timeSchedulerService TimeSchedulerService
   */
  @Inject
  public LocalScheduleManager(TransactionRunner transactionRunner, MessagingService messagingService,
      CConfiguration cConf, TimeSchedulerService timeSchedulerService) {
    super(transactionRunner, messagingService, cConf);
    this.timeSchedulerService = timeSchedulerService;
  }

  @Override
  public void addSchedules(Iterable<? extends ProgramSchedule> schedules)
      throws BadRequestException, NotFoundException, IOException, ConflictException {
    for (ProgramSchedule schedule : schedules) {
      if (!schedule.getProgramId().getType().equals(ProgramType.WORKFLOW)) {
        throw new BadRequestException(String.format(
            "Cannot schedule program %s of type %s: Only workflows can be scheduled",
            schedule.getProgramId().getProgram(), schedule.getProgramId().getType()));
      }
    }

    try {
      TransactionRunners.run(transactionRunner, context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        ProfileStore profileStore = ProfileStore.get(context);
        long updatedTime = store.addSchedules(schedules);
        for (ProgramSchedule schedule : schedules) {
          if (schedule.getProperties() != null) {
            Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(
                schedule.getProgramId().getNamespaceId(), schedule.getProperties());
            if (profile.isPresent()) {
              ProfileId profileId = profile.get();
              if (profileStore.getProfile(profileId).getStatus() == ProfileStatus.DISABLED) {
                throw new ProfileConflictException(
                    String.format("Profile %s in namespace %s is disabled. It cannot "
                            + "be assigned to schedule %s",
                        profileId.getProfile(), profileId.getNamespace(),
                        schedule.getName()), profileId);
              }
            }
          }
          try {
            timeSchedulerService.addProgramSchedule(schedule);
          } catch (SchedulerException e) {
            LOG.error("Exception occurs when adding schedule {}", schedule, e);
            throw new RuntimeException(e);
          }
        }
        for (ProgramSchedule schedule : schedules) {
          ScheduleId scheduleId = schedule.getScheduleId();

          // If the added properties contains profile assignment, add the assignment.
          Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(
              scheduleId.getNamespaceId(),
              schedule.getProperties());
          if (profileId.isPresent()) {
            profileStore.addProfileAssignment(profileId.get(), scheduleId);
          }
        }
        // Publish the messages at the end of transaction.
        for (ProgramSchedule schedule : schedules) {
          adminEventPublisher.publishScheduleCreation(schedule.getScheduleId(), updatedTime);
        }
        return null;
      }, Exception.class);
    } catch (NotFoundException | ProfileConflictException | AlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId)
      throws NotFoundException, BadRequestException, IOException, ConflictException {
    TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      ProfileStore profileStore = ProfileStore.get(context);
      JobQueueTable queue = JobQueueTable.getJobQueue(context, cConf);
      long deleteTime = System.currentTimeMillis();
      List<ProgramSchedule> toNotify = new ArrayList<>();
      ProgramSchedule schedule = store.getSchedule(scheduleId);
      timeSchedulerService.deleteProgramSchedule(schedule);
      queue.markJobsForDeletion(scheduleId, deleteTime);
      toNotify.add(schedule);
      // If the deleted schedule has properties with profile assignment, remove the assignment.
      Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(
          scheduleId.getNamespaceId(),
          schedule.getProperties());
      if (profileId.isPresent()) {
        try {
          profileStore.removeProfileAssignment(profileId.get(), scheduleId);
        } catch (NotFoundException e) {
          // This should not happen since the profile cannot be deleted if there is a schedule who is using it.
          LOG.warn("Unable to find the profile {} when deleting schedule {}, "
              + "skipping assignment deletion.", profileId.get(), scheduleId);
        }
      }
      store.deleteSchedule(scheduleId);
      toNotify.forEach(adminEventPublisher::publishScheduleDeletion);
      return null;
    }, NotFoundException.class);
  }
}
