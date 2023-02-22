/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.service.RetryOnStartFailureService;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueueTable;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.store.profile.ProfileStore;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Service that implements the Scheduler interface. This implements the actual Scheduler using
 * a {@link ProgramScheduleStoreDataset}
 */
public class CoreSchedulerService extends AbstractIdleService implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(CoreSchedulerService.class);
  private static final Gson GSON = new Gson();

  private final CountDownLatch startedLatch;
  private final Service internalService;
  private final TimeSchedulerService timeSchedulerService;
  private final AdminEventPublisher adminEventPublisher;
  private final CConfiguration cConf;
  private final Store appMetaStore;
  private final Impersonator impersonator;
  private final TransactionRunner transactionRunner;

  @Inject
  CoreSchedulerService(TimeSchedulerService timeSchedulerService,
                       ScheduleNotificationSubscriberService scheduleNotificationSubscriberService,
                       ConstraintCheckerService constraintCheckerService,
                       MessagingService messagingService,
                       CConfiguration cConf, Store store, Impersonator impersonator,
                       TransactionRunner transactionRunner) {
    this.startedLatch = new CountDownLatch(1);
    MultiThreadMessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    this.timeSchedulerService = timeSchedulerService;
    this.cConf = cConf;
    this.appMetaStore = store;
    this.impersonator = impersonator;
    this.transactionRunner = transactionRunner;
    // Use a retry on failure service to make it resilience to transient service unavailability during startup
    this.internalService = new RetryOnStartFailureService(() -> new AbstractIdleService() {

      @Override
      protected Executor executor(final State state) {
        return command -> new Thread(command, "core scheduler service " + state).start();
      }

      @Override
      protected void startUp() {
        timeSchedulerService.startAndWait();
        cleanupJobs();
        constraintCheckerService.startAndWait();
        scheduleNotificationSubscriberService.startAndWait();
        startedLatch.countDown();
        LOG.info("Started core scheduler service.");
      }

      @Override
      protected void shutDown() {
        scheduleNotificationSubscriberService.stopAndWait();
        constraintCheckerService.stopAndWait();
        timeSchedulerService.stopAndWait();
        LOG.info("Stopped core scheduler service.");
      }
    }, io.cdap.cdap.common.service.RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS));
    this.adminEventPublisher = new AdminEventPublisher(cConf, messagingContext);
  }

  // Attempts to remove all jobs that are in PENDING_LAUNCH state.
  // These are jobs that were about to be launched, but the scheduler shut down or crashed after the job was marked
  // PENDING_LAUNCH, but before they were actually launched.
  // This should only be called at startup.
  private void cleanupJobs() {
    try {
      TransactionRunners.run(transactionRunner, context -> {
        JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, cConf);
        try (CloseableIterator<Job> jobIter = jobQueue.fullScan()) {
          LOG.info("Cleaning up jobs in state {}.", Job.State.PENDING_LAUNCH);
          while (jobIter.hasNext()) {
            Job job = jobIter.next();
            if (job.getState() == Job.State.PENDING_LAUNCH) {
              LOG.warn("Removing job because it was left in state {} from a previous run of the scheduler: {} .",
                       Job.State.PENDING_LAUNCH, job);
              jobQueue.deleteJob(job);
            }
          }
        }
      }, TransactionException.class);
    } catch (TransactionException exception) {
      LOG.warn("Failed to cleanup jobs upon startup.", exception);
    }
  }

  /**
   * Waits for this scheduler completely started and functional.
   *
   * @param timeout maximum timeout to wait for
   * @param timeoutUnit unit for the timeout
   *
   * @throws InterruptedException if the current thread is being interrupted
   * @throws TimeoutException if the scheduler is not yet functional until the give timeout time passed
   * @throws IllegalStateException if this scheduler is not yet started
   */
  @VisibleForTesting
  public void waitUntilFunctional(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
    if (!isRunning()) {
      throw new IllegalStateException(getClass().getSimpleName()
                                        + " is not running. Cannot wait for it to be functional.");
    }
    if (!startedLatch.await(timeout, timeoutUnit)) {
      throw new TimeoutException(getClass().getSimpleName()
                                   + " is not completely started after " + timeout + " " + timeoutUnit);
    }
  }

  /**
   * Checks if the scheduler started completely and is functional.
   *
   * @throws ServiceUnavailableException if the scheduler is not yet functional
   */
  private void checkStarted() {
    if (!Uninterruptibles.awaitUninterruptibly(startedLatch, 0, TimeUnit.SECONDS)) {
      throw new ServiceUnavailableException("Core scheduler");
    }
  }

  @Override
  protected void startUp() throws Exception {
    internalService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    internalService.stopAndWait();
  }

  @Override
  public void addSchedule(ProgramSchedule schedule)
    throws ProfileConflictException, BadRequestException, NotFoundException, AlreadyExistsException {
    addSchedules(Collections.singleton(schedule));
  }

  @Override
  public void addSchedules(Iterable<? extends ProgramSchedule> schedules)
    throws ProfileConflictException, BadRequestException, NotFoundException, AlreadyExistsException {
    checkStarted();
    for (ProgramSchedule schedule: schedules) {
      if (!schedule.getProgramReference().getType().equals(ProgramType.WORKFLOW)) {
        throw new BadRequestException(String.format(
          "Cannot schedule program %s of type %s: Only workflows can be scheduled",
          schedule.getProgramReference().getProgram(), schedule.getProgramReference().getType()));
      }
    }
    try {
      execute((StoreAndProfileTxRunnable<Void, Exception>) (store, profileDataset) -> {
        long updatedTime = store.addSchedules(schedules);
        for (ProgramSchedule schedule : schedules) {
          if (schedule.getProperties() != null) {
            Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(
              schedule.getProgramReference().getNamespaceId(), schedule.getProperties());
            if (profile.isPresent()) {
              ProfileId profileId = profile.get();
              if (profileDataset.getProfile(profileId).getStatus() == ProfileStatus.DISABLED) {
                throw new ProfileConflictException(String.format("Profile %s in namespace %s is disabled. It cannot " +
                                                                   "be assigned to schedule %s",
                                                                 profileId.getProfile(), profileId.getNamespace(),
                                                                 schedule.getName()), profileId);
              }
            }
          }
          try {
            // TODO: [CDAP-11576] need to clean up the inconsistent state if this operation fails
            timeSchedulerService.addProgramSchedule(schedule);
          } catch (SchedulerException e) {
            // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
            // Need better error handling
            LOG.error("Exception occurs when adding schedule {}", schedule, e);
            throw new RuntimeException(e);
          }
        }
        for (ProgramSchedule schedule : schedules) {
          ScheduleId scheduleId = schedule.getScheduleId();

          // if the added properties contains profile assignment, add the assignment
          Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(),
                                                                               schedule.getProperties());
          if (profileId.isPresent()) {
            profileDataset.addProfileAssignment(profileId.get(), scheduleId);
          }
        }
        // publish the messages at the end of transaction
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
  public void updateSchedule(ProgramSchedule schedule)
    throws NotFoundException, BadRequestException, ProfileConflictException {
    checkStarted();
    ProgramScheduleStatus previousStatus = getScheduleStatus(schedule.getScheduleId());
    deleteSchedule(schedule.getScheduleId());
    try {
      addSchedule(schedule);
    } catch (AlreadyExistsException e) {
      // Should never reach here because we just deleted it
      throw new IllegalStateException(
        "Schedule '" + schedule.getScheduleId() + "' already exists despite just being deleted.");
    }
    // if the schedule was previously enabled, it should still/again enabled be after the update
    if (ProgramScheduleStatus.SCHEDULED == previousStatus) {
      try {
        enableSchedule(schedule.getScheduleId());
      } catch (ConflictException e) {
        // Should never reach here because we just added this
        throw new IllegalStateException(
          "Schedule '" + schedule.getScheduleId() + "' already enabled despite just being added.");
      }
    }
  }

  @Override
  public void enableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException {
    checkStarted();
    try {
      execute((StoreTxRunnable<Void, Exception>) store -> {
        enableScheduleInternal(store, scheduleId);
        return null;
      }, Exception.class);
    } catch (NotFoundException | ConflictException e) {
      throw e;
    } catch (SchedulerException e) {
      // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
      throw new RuntimeException("Exception occurs when enabling schedule " + scheduleId, e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void disableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException {
    checkStarted();
    try {
      execute((StoreAndQueueTxRunnable<Void, Exception>) (store, queue) -> {
        ProgramScheduleRecord record = store.getScheduleRecord(scheduleId);
        if (ProgramScheduleStatus.SCHEDULED != record.getMeta().getStatus()) {
          throw new ConflictException("Schedule '" + scheduleId + "' is already disabled");
        }
        timeSchedulerService.suspendProgramSchedule(record.getSchedule());
        store.updateScheduleStatus(scheduleId, ProgramScheduleStatus.SUSPENDED);
        queue.markJobsForDeletion(scheduleId, System.currentTimeMillis());
        return null;
      }, Exception.class);
    } catch (NotFoundException | ConflictException e) {
      throw e;
    } catch (SchedulerException e) {
      // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
      throw new RuntimeException("Exception occurs when enabling schedule " + scheduleId, e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId) throws NotFoundException {
    deleteSchedules(Collections.singleton(scheduleId));
  }

  private void deleteSchedulesInScheduler(List<ProgramSchedule> schedules) {
    for (ProgramSchedule schedule : schedules) {
      try {
        // TODO: [CDAP-11576] need to clean up the inconsistent state if this operation fails
        timeSchedulerService.deleteProgramSchedule(schedule);
      } catch (SchedulerException e) {
        // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
        // Need better error handling
        LOG.error("Exception occurs when deleting schedule {}", schedule, e);
        throw new RuntimeException(e);
      }
    }
  }

  private void deleteScheduleInScheduler(ProgramSchedule schedule) throws NotFoundException {
    try {
      // TODO: [CDAP-11576] need to clean up the inconsistent state if this operation fails
      timeSchedulerService.deleteProgramSchedule(schedule);
    } catch (SchedulerException e) {
      // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw NotFoundException.
      // Need better error handling
      LOG.error("Exception occurs when deleting schedule {}", schedule, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {
    checkStarted();
    execute((StoreQueueAndProfileTxRunnable<Void, Exception>) (store, queue, profileDataset) -> {
      long deleteTime = System.currentTimeMillis();
      List<ProgramSchedule> toNotify = new ArrayList<>();
      for (ScheduleId scheduleId : scheduleIds) {
        ProgramSchedule schedule = store.getSchedule(scheduleId);
        deleteScheduleInScheduler(schedule);
        queue.markJobsForDeletion(scheduleId, deleteTime);
        toNotify.add(schedule);
        // if the deleted schedule has properties with profile assignment, remove the assignment
        Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(),
                                                                             schedule.getProperties());
        if (profileId.isPresent()) {
          try {
            profileDataset.removeProfileAssignment(profileId.get(), scheduleId);
          } catch (NotFoundException e) {
            // this should not happen since the profile cannot be deleted if there is a schedule who is using it
            LOG.warn("Unable to find the profile {} when deleting schedule {}, " +
                       "skipping assignment deletion.", profileId.get(), scheduleId);
          }
        }
      }
      store.deleteSchedules(scheduleIds, deleteTime);
      toNotify.forEach(adminEventPublisher::publishScheduleDeletion);
      return null;
    }, NotFoundException.class);
  }

  @Override
  public void deleteSchedules(ApplicationReference appReference) {
    checkStarted();
    execute((StoreQueueAndProfileTxRunnable<Void, Exception>) (store, queue, profileDataset) -> {
      long deleteTime = System.currentTimeMillis();
      List<ProgramSchedule> schedules = store.listSchedules(appReference);
      deleteSchedulesInScheduler(schedules);
      List<ScheduleId> deleted = store.deleteSchedules(appReference, deleteTime);
      for (ScheduleId scheduleId : deleted) {
        queue.markJobsForDeletion(scheduleId, deleteTime);
      }
      for (ProgramSchedule programSchedule : schedules) {
        ScheduleId scheduleId = programSchedule.getScheduleId();
        // if the deleted schedule has properties with profile assignment, remove the assignment
        Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(),
                                                                             programSchedule.getProperties());
        if (profileId.isPresent()) {
          try {
            profileDataset.removeProfileAssignment(profileId.get(), scheduleId);
          } catch (NotFoundException e) {
            // this should not happen since the profile cannot be deleted if there is a schedule who is using it
            LOG.warn("Unable to find the profile {} when deleting schedule {}, " +
                       "skipping assignment deletion.", profileId.get(), scheduleId);
          }
        }
      }
      schedules.forEach(adminEventPublisher::publishScheduleDeletion);
      return null;
    }, RuntimeException.class);
  }

  @Override
  public void deleteSchedules(ProgramReference programReference) {
    checkStarted();
    execute((StoreQueueAndProfileTxRunnable<Void, Exception>) (store, queue, profileDataset) -> {
      long deleteTime = System.currentTimeMillis();
      List<ProgramSchedule> schedules = store.listSchedules(programReference);
      deleteSchedulesInScheduler(schedules);
      List<ScheduleId> deleted = store.deleteSchedules(programReference, deleteTime);
      for (ScheduleId scheduleId : deleted) {
        queue.markJobsForDeletion(scheduleId, deleteTime);
      }
      for (ProgramSchedule programSchedule : schedules) {
        ScheduleId scheduleId = programSchedule.getScheduleId();
        // if the deleted schedule has properties with profile assignment, remove the assignment
        Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(),
                                                                             programSchedule.getProperties());
        if (profileId.isPresent()) {
          try {
            profileDataset.removeProfileAssignment(profileId.get(), scheduleId);
          } catch (NotFoundException e) {
            // this should not happen since the profile cannot be deleted if there is a schedule who is using it
            LOG.warn("Unable to find the profile {} when deleting schedule {}, " +
                       "skipping assignment deletion.", profileId.get(), scheduleId);
          }
        }
      }
      schedules.forEach(adminEventPublisher::publishScheduleDeletion);
      return null;
    }, RuntimeException.class);
  }

  @Override
  public void modifySchedulesTriggeredByDeletedProgram(ProgramReference programRef) {
    checkStarted();
    execute((StoreAndQueueTxRunnable<Void, Exception>) (store, queue) -> {
      List<ProgramSchedule> deletedSchedules = store.modifySchedulesTriggeredByDeletedProgram(programRef);
      deletedSchedules.forEach(adminEventPublisher::publishScheduleDeletion);
      return null;
    }, RuntimeException.class);
  }

  @Override
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException {
    checkStarted();
    return execute(store -> store.getSchedule(scheduleId), NotFoundException.class);
  }

  @Override
  public ProgramScheduleRecord getScheduleRecord(ScheduleId scheduleId) throws NotFoundException {
    checkStarted();
    return execute(store -> store.getScheduleRecord(scheduleId), NotFoundException.class);
  }

  @Override
  public ProgramScheduleStatus getScheduleStatus(ScheduleId scheduleId) throws NotFoundException {
    checkStarted();
    return execute(store -> store.getScheduleRecord(scheduleId).getMeta().getStatus(), NotFoundException.class);
  }

  @Override
  public List<ProgramSchedule> listSchedules(ApplicationReference appReference) {
    checkStarted();
    return execute(store -> store.listSchedules(appReference), RuntimeException.class);
  }

  @Override
  public List<ProgramSchedule> listSchedules(ProgramReference programReference) {
    checkStarted();
    return execute(store -> store.listSchedules(programReference), RuntimeException.class);
  }

  @Override
  public List<ProgramSchedule> listSchedules(NamespaceId namespaceId,
                                             Predicate<ProgramSchedule> filter) {
    checkStarted();
    return execute(store -> store.listSchedules(namespaceId, filter).stream()
                     .map(this::getProgramScheduleWithUserAndArtifactId).collect(Collectors.toList()),
                   RuntimeException.class);
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ApplicationReference applicationReference) {
    checkStarted();
    return execute(store -> store.listScheduleRecords(applicationReference), RuntimeException.class);
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ProgramReference programReference) {
    checkStarted();
    return execute(store -> store.listScheduleRecords(programReference), RuntimeException.class);
  }

  @Override
  public Collection<ProgramScheduleRecord> findSchedules(String triggerKey) {
    checkStarted();
    return execute(store -> store.findSchedules(triggerKey), RuntimeException.class);
  }

  @Override
  public void reEnableSchedules(NamespaceId namespaceId, long startTimeMillis, long endTimeMillis)
    throws ConflictException {
    checkStarted();
    try {
      execute((StoreTxRunnable<Void, Exception>) store -> {
        List<ProgramSchedule> schedules = store.listSchedulesSuspended(namespaceId, startTimeMillis, endTimeMillis);
        List<ScheduleId> scheduleIds =
          schedules.stream().map(schedule -> schedule.getScheduleId()).collect(Collectors.toList());
        for (ScheduleId scheduleId : scheduleIds) {
          enableScheduleInternal(store, scheduleId);
        }
        return null;
      }, Exception.class);
    } catch (ConflictException e) {
      throw e;
    } catch (SchedulerException | NotFoundException e) {
      // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
      throw new RuntimeException("Exception occurs when enabling schedules", e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a copy of the given {@link ProgramSchedule} and add user and artifact ID in the schedule properties
   * TODO CDAP-13662 - move logic to find artifactId and userId to dashboard service and remove this method
   */
  private ProgramSchedule getProgramScheduleWithUserAndArtifactId(ProgramSchedule schedule) {
    Map<String, String> additionalProperties = new HashMap<>();
    // add artifact id to the schedule property
    ProgramDescriptor programDescriptor;
    try {
      // Since schedule is versionless (always has "-SNAPSHOT" version)
      // We call with ProgramReference
      programDescriptor = appMetaStore.loadProgram(schedule.getProgramReference());
    } catch (Exception e) {
      LOG.error("Exception occurs when looking up program descriptor for program {} in schedule {}",
                schedule.getProgramReference(), schedule, e);
      throw new RuntimeException(String.format("Exception occurs when looking up program descriptor for" +
                                                 " program %s in schedule %s", schedule.getProgramReference(),
                                               schedule), e);
    }
    additionalProperties.put(ProgramOptionConstants.ARTIFACT_ID,
                             GSON.toJson(programDescriptor.getArtifactId().toApiArtifactId()));

    String userId;
    try {
      userId = impersonator.getUGI(schedule.getProgramReference()).getUserName();
    } catch (AccessException e) {
      LOG.error("Exception occurs when looking up user group information for program {} in schedule {}",
                schedule.getProgramReference(), schedule, e);
      throw new RuntimeException(String.format("Exception occurs when looking up user group information for" +
                                                 " program %s in schedule %s", schedule.getProgramReference(),
                                               schedule), e);
    }
    // add the user name to the schedule property
    additionalProperties.put(ProgramOptionConstants.USER_ID, userId);
    // make a copy of the existing schedule properties and add the additional properties in the copy
    Map<String, String> newProperties = new HashMap<>(schedule.getProperties());
    newProperties.putAll(additionalProperties);
    // construct a copy of the schedule with the additional properties added
    return new ProgramSchedule(schedule.getName(), schedule.getDescription(), schedule.getProgramReference(),
                               newProperties, schedule.getTrigger(), schedule.getConstraints(),
                               schedule.getTimeoutMillis());
  }

  private void enableScheduleInternal(ProgramScheduleStoreDataset store, ScheduleId scheduleId)
    throws IOException, NotFoundException, ConflictException, SchedulerException {
    ProgramScheduleRecord record = store.getScheduleRecord(scheduleId);
    if (ProgramScheduleStatus.SUSPENDED != record.getMeta().getStatus()) {
      throw new ConflictException("Schedule '" + scheduleId + "' is already enabled");
    }
    timeSchedulerService.resumeProgramSchedule(record.getSchedule());
    store.updateScheduleStatus(scheduleId, ProgramScheduleStatus.SCHEDULED);
  }

  private interface StoreTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store) throws T;
  }

  private interface StoreAndQueueTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store, JobQueueTable jobQueue) throws T;
  }

  private interface StoreAndProfileTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store, ProfileStore profileStore) throws T;
  }

  private interface StoreQueueAndProfileTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store, JobQueueTable jobQueue, ProfileStore profileStore) throws T;
  }

  private <V, T extends Exception> V execute(StoreTxRunnable<V, ? extends Exception> runnable,
                                             Class<? extends T> tClass) throws T {
    return TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      return runnable.run(store);
    }, tClass);
  }

  @SuppressWarnings("UnusedReturnValue")
  private <V, T extends Exception> V execute(StoreAndQueueTxRunnable<V, ? extends Exception> runnable,
                                             Class<? extends T> tClass) throws T {
    return TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      JobQueueTable queue = JobQueueTable.getJobQueue(context, cConf);
      return runnable.run(store, queue);
    }, tClass);
  }

  @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
  private <V, T extends Exception> V execute(StoreAndProfileTxRunnable<V, ? extends Exception> runnable,
                                             Class<? extends T> tClass) throws T {
    return TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      ProfileStore profileStore = ProfileStore.get(context);
      return runnable.run(store, profileStore);
    }, tClass);
  }

  @SuppressWarnings("UnusedReturnValue")
  private <V, T extends Exception> V execute(StoreQueueAndProfileTxRunnable<V, ? extends Exception> runnable,
                                             Class<? extends T> tClass) throws T {
    return TransactionRunners.run(transactionRunner, context -> {
      ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
      ProfileStore profileStore = ProfileStore.get(context);
      JobQueueTable queue = JobQueueTable.getJobQueue(context, cConf);
      return runnable.run(store, queue, profileStore);
    }, tClass);
  }
}
