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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Service that implements the Scheduler interface. This implements the actual Scheduler using
 * a {@link ProgramScheduleStoreDataset}
 */
public class CoreSchedulerService extends AbstractIdleService implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(CoreSchedulerService.class);

  private final Transactional transactional;
  private final Service internalService;
  private final DatasetFramework datasetFramework;
  private final SchedulerService scheduler;

  @Inject
  CoreSchedulerService(TransactionSystemClient txClient, final DatasetFramework datasetFramework,
                       final SchedulerService schedulerService,
                       final NotificationSubscriberService notificationSubscriberService,
                       final ConstraintCheckerService constraintCheckerService,
                       final NamespaceQueryAdmin namespaceQueryAdmin, final Store store) {
    this.datasetFramework = datasetFramework;
    DynamicDatasetCache datasetCache = new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, Schedulers.STORE_DATASET_ID.getParent(),
                                                                   Collections.<String, String>emptyMap(), null, null);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(datasetCache), RetryStrategies.retryOnConflict(10, 100L));

    this.scheduler = schedulerService;
    // Use a retry on failure service to make it resilience to transient service unavailability during startup
    this.internalService = new RetryOnStartFailureService(new Supplier<Service>() {
      @Override
      public Service get() {
        return new AbstractIdleService() {
          @Override
          protected void startUp() throws Exception {
            if (!datasetFramework.hasInstance(Schedulers.STORE_DATASET_ID)) {
              datasetFramework.addInstance(Schedulers.STORE_TYPE_NAME,
                                           Schedulers.STORE_DATASET_ID, DatasetProperties.EMPTY);
            }
            schedulerService.startAndWait();
            migrateSchedules(namespaceQueryAdmin, store);
            constraintCheckerService.startAndWait();
            notificationSubscriberService.startAndWait();
          }

          @Override
          protected void shutDown() throws Exception {
            notificationSubscriberService.stopAndWait();
            constraintCheckerService.stopAndWait();
            schedulerService.stopAndWait();
          }
        };
      }
    }, co.cask.cdap.common.service.RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS));
  }

  private void migrateSchedules(final NamespaceQueryAdmin namespaceQueryAdmin, final Store appMetaStore)
    throws Exception {

    List<NamespaceMeta> namespaceMetas = namespaceQueryAdmin.list();
    boolean migrateComplete = execute(new StoreTxRunnable<Boolean, RuntimeException>() {
      @Override
      public Boolean run(ProgramScheduleStoreDataset store) {
        return store.isMigrationComplete();
      }
    }, RuntimeException.class);
    if (migrateComplete) {
      return; // no need to migrate if migration is complete
    }
    String completedNamespace = null;
    for (NamespaceMeta namespaceMeta : namespaceMetas) {
      final NamespaceId namespaceId = namespaceMeta.getNamespaceId();
      // Since namespaces are listed in lexicographical order, if the completedNamespace is larger than
      // the current namespace lexicographically, then the current namespace is already migrated. Skip this namespace.
      if (completedNamespace != null && completedNamespace.compareTo(namespaceId.toString()) > 0) {
        continue;
      }
      completedNamespace = execute(new StoreTxRunnable<String, RuntimeException>() {
        @Override
        public String run(ProgramScheduleStoreDataset store) {
          return store.migrateFromAppMetadataStore(namespaceId, appMetaStore);
        }
      }, RuntimeException.class);
    }
    // Set migration complete after migrating all namespaces
    execute(new StoreTxRunnable<Void, RuntimeException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store) {
        store.setMigrationComplete();
        return null;
      }
    }, RuntimeException.class);
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
  public void addSchedule(ProgramSchedule schedule) throws AlreadyExistsException, BadRequestException {
    addSchedules(Collections.singleton(schedule));
  }

  @Override
  public void addSchedules(final Iterable<? extends ProgramSchedule> schedules)
    throws AlreadyExistsException, BadRequestException {
    for (ProgramSchedule schedule: schedules) {
      if (!schedule.getProgramId().getType().equals(ProgramType.WORKFLOW)) {
        throw new BadRequestException(String.format(
          "Cannot schedule program %s of type %s: Only workflows can be scheduled",
          schedule.getProgramId().getProgram(), schedule.getProgramId().getType()));
      }
    }
    execute(new StoreTxRunnable<Void, AlreadyExistsException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store) throws AlreadyExistsException {
        store.addSchedules(schedules);
        for (ProgramSchedule schedule : schedules) {
          try {
            // TODO: [CDAP-11576] need to clean up the inconsistent state if this operation fails
            scheduler.addProgramSchedule(schedule);
          } catch (SchedulerException e) {
            // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw RuntimeException.
            // Need better error handling
            LOG.error("Exception occurs when adding schedule {}", schedule, e);
            throw new RuntimeException(e);
          }
        }
        return null;
      }
    }, AlreadyExistsException.class);

  }

  @Override
  public void updateSchedule(final ProgramSchedule schedule) throws NotFoundException, BadRequestException {
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
  public void enableSchedule(final ScheduleId scheduleId) throws NotFoundException, ConflictException {
    try {
      execute(new StoreTxRunnable<Void, Exception>() {
        @Override
        public Void run(ProgramScheduleStoreDataset store)
          throws NotFoundException, ConflictException, SchedulerException {
          ProgramScheduleRecord record = store.getScheduleRecord(scheduleId);
          if (ProgramScheduleStatus.SUSPENDED != record.getMeta().getStatus()) {
            throw new ConflictException("Schedule '" + scheduleId + "' is already enabled");
          }
          scheduler.resumeProgramSchedule(record.getSchedule());
          store.updateScheduleStatus(scheduleId, ProgramScheduleStatus.SCHEDULED);
          return null;
        }
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
  public void disableSchedule(final ScheduleId scheduleId) throws NotFoundException, ConflictException {
    try {
      execute(new StoreAndQueueTxRunnable<Void, Exception>() {
        @Override
        public Void run(ProgramScheduleStoreDataset store, JobQueueDataset queue)
          throws NotFoundException, ConflictException, SchedulerException {
          ProgramScheduleRecord record = store.getScheduleRecord(scheduleId);
          if (ProgramScheduleStatus.SCHEDULED != record.getMeta().getStatus()) {
            throw new ConflictException("Schedule '" + scheduleId + "' is already disabled");
          }
          scheduler.suspendProgramSchedule(record.getSchedule());
          store.updateScheduleStatus(scheduleId, ProgramScheduleStatus.SUSPENDED);
          queue.markJobsForDeletion(scheduleId, System.currentTimeMillis());
          return null;
        }
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
        scheduler.deleteProgramSchedule(schedule);
      } catch (Exception e) {
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
      scheduler.deleteProgramSchedule(schedule);
    } catch (SchedulerException e) {
      // TODO: [CDAP-11574] temporarily catch the SchedulerException and throw NotFoundException.
      // Need better error handling
      LOG.error("Exception occurs when deleting schedule {}", schedule, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSchedules(final Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {
    execute(new StoreAndQueueTxRunnable<Void, NotFoundException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store, JobQueueDataset queue) throws NotFoundException {
        long deleteTime = System.currentTimeMillis();
        for (ScheduleId scheduleId : scheduleIds) {
          deleteScheduleInScheduler(store.getSchedule(scheduleId));
          queue.markJobsForDeletion(scheduleId, deleteTime);
        }
        store.deleteSchedules(scheduleIds);
        return null;
      }
    }, NotFoundException.class);
  }

  @Override
  public void deleteSchedules(final ApplicationId appId) {
    execute(new StoreAndQueueTxRunnable<Void, RuntimeException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store, JobQueueDataset queue) {
        long deleteTime = System.currentTimeMillis();
        deleteSchedulesInScheduler(store.listSchedules(appId));
        List<ScheduleId> deleted = store.deleteSchedules(appId);
        for (ScheduleId scheduleId : deleted) {
          queue.markJobsForDeletion(scheduleId, deleteTime);
        }
        return null;
      }
    }, RuntimeException.class);
  }

  @Override
  public void deleteSchedules(final ProgramId programId) {
    execute(new StoreAndQueueTxRunnable<Void, RuntimeException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store, JobQueueDataset queue) {
        long deleteTime = System.currentTimeMillis();
        deleteSchedulesInScheduler(store.listSchedules(programId));
        List<ScheduleId> deleted = store.deleteSchedules(programId);
        for (ScheduleId scheduleId : deleted) {
          queue.markJobsForDeletion(scheduleId, deleteTime);
        }
        return null;
      }
    }, RuntimeException.class);
  }

  @Override
  public ProgramSchedule getSchedule(final ScheduleId scheduleId) throws NotFoundException {
    return execute(new StoreTxRunnable<ProgramSchedule, NotFoundException>() {
      @Override
      public ProgramSchedule run(ProgramScheduleStoreDataset store) throws NotFoundException {
        return store.getSchedule(scheduleId);
      }
    }, NotFoundException.class);
  }

  @Override
  public ProgramScheduleStatus getScheduleStatus(final ScheduleId scheduleId) throws NotFoundException {
    return execute(new StoreTxRunnable<ProgramScheduleStatus, NotFoundException>() {
      @Override
      public ProgramScheduleStatus run(ProgramScheduleStoreDataset store) throws NotFoundException {
        return store.getScheduleRecord(scheduleId).getMeta().getStatus();
      }
    }, NotFoundException.class);
  }

  @Override
  public List<ProgramSchedule> listSchedules(final ApplicationId appId) {
    return execute(new StoreTxRunnable<List<ProgramSchedule>, RuntimeException>() {
      @Override
      public List<ProgramSchedule> run(ProgramScheduleStoreDataset store) {
        return store.listSchedules(appId);
      }
    }, RuntimeException.class);
  }

  @Override
  public List<ProgramSchedule> listSchedules(final ProgramId programId) {
    return execute(new StoreTxRunnable<List<ProgramSchedule>, RuntimeException>() {
      @Override
      public List<ProgramSchedule> run(ProgramScheduleStoreDataset store) {
        return store.listSchedules(programId);
      }
    }, RuntimeException.class);
  }

  @Override
  public Collection<ProgramScheduleRecord> findSchedules(final String triggerKey) {
    return execute(new StoreTxRunnable<Collection<ProgramScheduleRecord>, RuntimeException>() {
      @Override
      public Collection<ProgramScheduleRecord> run(ProgramScheduleStoreDataset store) {
        return store.findSchedules(triggerKey);
      }
    }, RuntimeException.class);
  }

  private interface StoreTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store) throws T;
  }

  private interface StoreAndQueueTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store, JobQueueDataset jobQueue) throws T;
  }

  private <V, T extends Exception> V execute(final StoreTxRunnable<V, T> runnable,
                                             final Class<? extends T> tClass) throws T {
    try {
      return Transactions.execute(transactional, new TxCallable<V>() {
        @Override
        public V call(DatasetContext context) throws Exception {
          ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context, datasetFramework);
          return runnable.run(store);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, tClass);
    }
  }

  private <V, T extends Exception> V execute(final StoreAndQueueTxRunnable<V, T> runnable,
                                             final Class<? extends T> tClass) throws T {
    try {
      return Transactions.execute(transactional, new TxCallable<V>() {
        @Override
        public V call(DatasetContext context) throws Exception {
          ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context, datasetFramework);
          JobQueueDataset queue = Schedulers.getJobQueue(context, datasetFramework);
          return runnable.run(store, queue);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, tClass);
    }
  }
}
