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
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.List;

/**
 * Service that implements the Scheduler interface. This implements the actual Scheduler using
 * a {@link ProgramScheduleStoreDataset}
 */
public class CoreSchedulerService extends AbstractIdleService implements Scheduler {

  private final DatasetFramework datasetFramework;
  private final MultiThreadDatasetCache datasetCache;
  private final Transactional transactional;
  private final NotificationSubscriberService notificationSubscriberService;

  @Inject
  CoreSchedulerService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                       NotificationSubscriberService notificationSubscriberService) {
    this.datasetFramework = datasetFramework;
    this.datasetCache = new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                    txClient, Schedulers.STORE_DATASET_ID.getParent(),
                                                    Collections.<String, String>emptyMap(), null, null);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(datasetCache), RetryStrategies.retryOnConflict(10, 100L));
    this.notificationSubscriberService = notificationSubscriberService;
  }

  @Override
  protected void startUp() throws Exception {
    if (!datasetFramework.hasInstance(Schedulers.STORE_DATASET_ID)) {
      datasetFramework.addInstance(Schedulers.STORE_TYPE_NAME, Schedulers.STORE_DATASET_ID, DatasetProperties.EMPTY);
    }
    notificationSubscriberService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    notificationSubscriberService.stopAndWait();
  }

  @Override
  public void addSchedule(ProgramSchedule schedule) throws AlreadyExistsException {
    addSchedules(Collections.singleton(schedule));
  }

  @Override
  public void addSchedules(final Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException {
    execute(new StoreTxRunnable<Void, AlreadyExistsException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store) throws AlreadyExistsException {
        store.addSchedules(schedules);
        return null;
      }
    }, AlreadyExistsException.class);
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId) throws NotFoundException {
    deleteSchedules(Collections.singleton(scheduleId));
  }

  @Override
  public void deleteSchedules(final Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {
    execute(new StoreTxRunnable<Void, NotFoundException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store) throws NotFoundException {
        store.deleteSchedules(scheduleIds);
        return null;
      }
    }, NotFoundException.class);
  }

  @Override
  public void deleteSchedules(final ApplicationId appId) {
    execute(new StoreTxRunnable<Void, RuntimeException>() {
      @Override
      public Void run(ProgramScheduleStoreDataset store) {
        store.deleteSchedules(appId);
        return null;
      }
    }, null);
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
  public List<ProgramSchedule> listSchedules(final ApplicationId appId) {
    return execute(new StoreTxRunnable<List<ProgramSchedule>, RuntimeException>() {
      @Override
      public List<ProgramSchedule> run(ProgramScheduleStoreDataset store) {
        return store.listSchedules(appId);
      }
    }, null);
  }

  @Override
  public List<ProgramSchedule> listSchedules(final ProgramId programId) {
    return execute(new StoreTxRunnable<List<ProgramSchedule>, RuntimeException>() {
      @Override
      public List<ProgramSchedule> run(ProgramScheduleStoreDataset store) {
        return store.listSchedules(programId);
      }
    }, null);
  }

  private interface StoreTxRunnable<V, T extends Throwable> {
    V run(ProgramScheduleStoreDataset store) throws T;
  }

  private <V, T extends Exception> V execute(final StoreTxRunnable<V, T> runnable,
                                             final Class<? extends T> tClass) throws T {
    try {
      return Transactions.execute(transactional, new TxCallable<V>() {
        @Override
        public V call(DatasetContext context) throws Exception {
          ProgramScheduleStoreDataset store = context.getDataset(Schedulers.STORE_DATASET_ID.getDataset());
          return runnable.run(store);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, tClass);
    }
  }
}
