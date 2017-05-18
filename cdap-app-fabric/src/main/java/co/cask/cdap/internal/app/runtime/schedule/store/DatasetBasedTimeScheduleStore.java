/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.internal.app.runtime.schedule.DefaultSchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.commons.lang.SerializationUtils;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.simpl.RAMJobStore;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ScheduleStore extends from RAMJobStore and persists the trigger and schedule information into datasets.
 */
public class DatasetBasedTimeScheduleStore extends RAMJobStore {
  private static final String NAME = "TimeScheduleStore";
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.time.schedule");
  private static final byte[] COLUMN = Bytes.toBytes('c');
  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedTimeScheduleStore.class);
  private static final byte[] JOB_KEY = Bytes.toBytes("jobs");
  private static final byte[] TRIGGER_KEY = Bytes.toBytes("trigger");

  private final TransactionExecutorFactory factory;
  private final ScheduleStoreTableUtil tableUtil;

  private final CConfiguration cConf;

  private volatile LoadingCache<byte[], Boolean> upgradeCacheLoader;
  private Table table;

  @Inject
  DatasetBasedTimeScheduleStore(TransactionExecutorFactory factory, ScheduleStoreTableUtil tableUtil,
                                CConfiguration cConf) {
    this.tableUtil = tableUtil;
    this.factory = factory;
    this.cConf = cConf;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler) {
    super.initialize(loadHelper, schedSignaler);
    try {
      // See CDAP-7116
      setMisfireThreshold(cConf.getLong(Constants.Scheduler.CFG_SCHEDULER_MISFIRE_THRESHOLD_MS));
      initializeScheduleTable();
      readSchedulesFromPersistentStore();
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void initializeScheduleTable() throws IOException, DatasetManagementException {
    table = tableUtil.getMetaTable();
    Preconditions.checkNotNull(table, "Could not get dataset client for data set: %s",
                               ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME);
    upgradeCacheLoader = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
        // Use a new instance of table since Table is not thread safe
      .build(new UpgradeValueLoader(NAME, factory, table));
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException {
    super.storeJob(newJob, replaceExisting);
    persistJobAndTrigger(newJob, null);
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws JobPersistenceException {
    super.storeTrigger(newTrigger, replaceExisting);
    persistJobAndTrigger(null, newTrigger);
  }

  @Override
  public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
                                   boolean replace) throws JobPersistenceException {
    super.storeJobsAndTriggers(triggersAndJobs, replace);
    for (Map.Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
      persistJobAndTrigger(e.getKey(), null);
      for (Trigger trigger : e.getValue()) {
        persistJobAndTrigger(null, (OperableTrigger) trigger);
      }
    }
  }

  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
                                 throws JobPersistenceException {
    storeJob(newJob, true);
    storeTrigger(newTrigger, true);
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) {
    super.pauseTrigger(triggerKey);
    persistChangeOfState(triggerKey, Trigger.TriggerState.PAUSED);
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) {
    super.resumeTrigger(triggerKey);
    persistChangeOfState(triggerKey, Trigger.TriggerState.NORMAL);
  }

  @Override
  public boolean removeTrigger(TriggerKey triggerKey) {
    try {
      super.removeTrigger(triggerKey);
      executeDelete(triggerKey);
      return true;
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  @Override
  public boolean removeJob(JobKey jobKey) {
    try {
      super.removeJob(jobKey);
      executeDelete(jobKey);
      return true;
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private void executeDelete(final TriggerKey triggerKey) {
    try {
      final boolean needVersionLessDelete = !isUpgradeComplete();
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            if (needVersionLessDelete) {
              TriggerKey versionLessTriggerKey = removeAppVersion(triggerKey);
              if (versionLessTriggerKey != null) {
                removeTrigger(table, versionLessTriggerKey);
              }
            }
            removeTrigger(table, triggerKey);
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void executeDelete(final JobKey jobKey) {
    try {
      final boolean needVersionLessDelete = !isUpgradeComplete();
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            if (needVersionLessDelete) {
              JobKey versionLessJobKey = removeAppVersion(jobKey);
              if (versionLessJobKey != null) {
                removeJob(table, versionLessJobKey);
              }
            }
            removeJob(table, jobKey);
          }
        });
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private void persistChangeOfState(final TriggerKey triggerKey, final Trigger.TriggerState newTriggerState) {
    try {
      Preconditions.checkNotNull(triggerKey);
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            TriggerStatusV2 storedTriggerStatus = readTrigger(triggerKey);
            if (storedTriggerStatus != null) {
              // its okay to persist the same trigger back again since during pause/resume
              // operation the trigger does not change. We persist it here with just the new trigger state
              persistTrigger(table, storedTriggerStatus.trigger, newTriggerState);
            } else {
              LOG.warn("Trigger key {} was not found in {} while trying to persist its state to {}.",
                       triggerKey, ScheduleStoreTableUtil.SCHEDULE_STORE_DATASET_NAME, newTriggerState);
            }
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void persistJobAndTrigger(final JobDetail newJob, final OperableTrigger newTrigger) {
    try {
      Trigger.TriggerState triggerState = Trigger.TriggerState.NONE;
      if (newTrigger != null) {
        triggerState = super.getTriggerState(newTrigger.getKey());
      }
      final Trigger.TriggerState finalTriggerState = triggerState;
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            if (newJob != null) {
              persistJob(table, newJob);
              LOG.debug("Schedule: stored job with key {}", newJob.getKey());
            }
            if (newTrigger != null) {
              persistTrigger(table, newTrigger, finalTriggerState);
              LOG.debug("Schedule: stored trigger with key {}", newTrigger.getKey());
            }
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  // Persist the job information to dataset
  private void persistJob(Table table, JobDetail job) {
    byte[][] cols = new byte[1][];
    byte[][] values = new byte[1][];

    cols[0] = Bytes.toBytes(job.getKey().toString());
    values[0] = SerializationUtils.serialize(job);
    table.put(JOB_KEY, cols, values);
  }

  private void removeJob(Table table, JobKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.toString());
    table.delete(JOB_KEY, col);
  }

  private JobDetail readJob(Table table, JobKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.toString());
    Row row = table.get(JOB_KEY, col);
    byte[] bytes = null;
    if (!row.isEmpty()) {
      bytes = row.get(col[0]);
    }
    if (bytes != null) {
      return (JobDetail) SerializationUtils.deserialize(bytes);
    } else {
      return null;
    }
  }

  private void removeTrigger(Table table, TriggerKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.getName());
    table.delete(TRIGGER_KEY, col);
  }

  private TriggerStatusV2 readTrigger(TriggerKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.getName());
    Row result = table.get(TRIGGER_KEY, col);
    byte[] bytes = null;
    if (!result.isEmpty()) {
      bytes = result.get(col[0]);
    }
    if (bytes != null) {
      return (TriggerStatusV2) SerializationUtils.deserialize(bytes);
    } else {
      return null;
    }
  }

  // Persist the trigger information to dataset
  private void persistTrigger(Table table, OperableTrigger trigger,
                              Trigger.TriggerState state) {
    byte[][] cols = new byte[1][];
    byte[][] values = new byte[1][];

    cols[0] = Bytes.toBytes(trigger.getKey().getName());
    values[0] = SerializationUtils.serialize(new TriggerStatusV2(trigger, state));
    table.put(TRIGGER_KEY, cols, values);
  }

  // Get schedule information from persistent store
  private void readSchedulesFromPersistentStore() throws Exception {
    final List<JobDetail> jobs = Lists.newArrayList();
    final List<TriggerStatusV2> triggers = Lists.newArrayList();

    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Row result = table.get(JOB_KEY);
          if (!result.isEmpty()) {
            for (byte[] bytes : result.getColumns().values()) {
              JobDetail jobDetail = (JobDetail) SerializationUtils.deserialize(bytes);
              // If the job detail doesn't contain version id, add one.
              jobDetail = addDefaultAppVersionIfNeeded(jobDetail);

              LOG.debug("Schedule: Job with key {} found", jobDetail.getKey());
              jobs.add(jobDetail);
            }
          } else {
            LOG.debug("Schedule: No Jobs found in Job store");
          }

          result = table.get(TRIGGER_KEY);
          if (!result.isEmpty()) {
            for (byte[] bytes : result.getColumns().values()) {
              TriggerStatusV2 trigger = (TriggerStatusV2) SerializationUtils.deserialize(bytes);
              addDefaultAppVersionIfNeeded(trigger);
              if (trigger.state.equals(Trigger.TriggerState.NORMAL) ||
                trigger.state.equals(Trigger.TriggerState.PAUSED)) {
                triggers.add(trigger);
                LOG.debug("Schedule: trigger with key {} added", trigger.trigger.getKey());
              } else {
                LOG.debug("Schedule: trigger with key {} and state {} skipped", trigger.trigger.getKey(),
                          trigger.state);
              }
            }
          } else {
            LOG.debug("Schedule: No triggers found in job store");
          }
        }
      });

    for (JobDetail job : jobs) {
      super.storeJob(job, true);
    }

    for (TriggerStatusV2 trigger : triggers) {
      super.storeTrigger(trigger.trigger, true);
      // if the trigger was paused then pause it back. This is needed because the state of the trigger is not a
      // property associated with the trigger.
      // Its fine to do it this way and we will not run into issues where a triggers get fired before its paused
      // because the scheduler is actually not started at this point.
      if (trigger.state == Trigger.TriggerState.PAUSED) {
        super.pauseTrigger(trigger.trigger.getKey());
      }
    }
  }

  // Returns true if the upgrade flag is set. Upgrade could have completed earlier than this since this flag is
  // updated by a cache loader which has fixed expiry time.
  public boolean isUpgradeComplete() {
    if (upgradeCacheLoader == null) {
      return false;
    }
    return upgradeCacheLoader.getUnchecked(APP_VERSION_UPGRADE_KEY);
  }

  /**
   * Method to add version to row key in SchedulerStore.
   *
   * @throws InterruptedException
   * @throws IOException
   * @throws DatasetManagementException
   */
  public void upgrade() throws InterruptedException, IOException, DatasetManagementException {
    while (true) {
      try {
        initializeScheduleTable();
        break;
      } catch (Exception ex) {
        // Expected if the cdap services are not up.
        TimeUnit.SECONDS.sleep(10);
      }
    }

    if (isUpgradeComplete()) {
      LOG.info("{} is already upgraded.", NAME);
      return;
    }

    final AtomicInteger sleepTimeInSecs = new AtomicInteger(60);
    final AtomicInteger tries = new AtomicInteger(0);
    LOG.info("Starting upgrade of {}.", NAME);
    while (!isUpgradeComplete()) {
      sleepTimeInSecs.set(60);
      try {
        factory.createExecutor(ImmutableList.of((TransactionAware) table))
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() {
              upgradeJobs(table);
              upgradeTriggers(table);
              // Upgrade is complete. Mark that app version upgrade is complete in the table.
              table.put(APP_VERSION_UPGRADE_KEY, COLUMN, Bytes.toBytes(ProjectInfo.getVersion().toString()));
            }
          });
      } catch (TransactionFailureException e) {
        if (e instanceof TransactionConflictException) {
          LOG.debug("Upgrade step faced Transaction Conflict exception. Retrying operation now.", e);
          sleepTimeInSecs.set(10);
        } else {
          LOG.error("Upgrade step faced exception. Will retry operation after some delay.", e);
          sleepTimeInSecs.set(60);
        }
      }

      if (tries.incrementAndGet() > 500) {
        LOG.warn("Could not complete upgrade of {}, tried for 500 times", NAME);
        return;
      }
      TimeUnit.SECONDS.sleep(sleepTimeInSecs.get());
    }
    LOG.info("Upgrade of {} is complete.", NAME);
  }

  @VisibleForTesting
  TriggerKey removeAppVersion(TriggerKey key) {
    // New TriggerKey name has format = namespace:application:version:type:program:schedule
    String versionLessJobName = ScheduleUpgradeUtil.splitAndRemoveDefaultVersion(key.getName(), 6, 2);
    if (versionLessJobName == null) {
      return null;
    }
    return new TriggerKey(versionLessJobName, key.getGroup());
  }

  @VisibleForTesting
  JobKey removeAppVersion(JobKey origJobKey) {
    // New JobKey name has format = namespace:application:version:type:program
    String versionLessJobName = ScheduleUpgradeUtil.splitAndRemoveDefaultVersion(origJobKey.getName(), 5, 2);
    if (versionLessJobName == null) {
      return null;
    }
    return new JobKey(versionLessJobName, origJobKey.getGroup());
  }

  private JobDetail addDefaultAppVersionIfNeeded(JobDetail origJobDetail) {
    String jobName = origJobDetail.getKey().getName();
    String[] splits = jobName.split(":");
    if (splits.length != 4) {
      // It already has version string in the job key. Hence return the origJobDetail
      return origJobDetail;
    }

    // Old TriggerKey name has format = namespace:application:type:program
    String newJobName = ScheduleUpgradeUtil.getNameWithDefaultVersion(splits, 2);
    return JobBuilder.newJob(DefaultSchedulerService.ScheduledJob.class)
      .withIdentity(newJobName, origJobDetail.getKey().getGroup())
      .storeDurably(origJobDetail.isDurable())
      .build();
  }

  private boolean addDefaultAppVersionIfNeeded(TriggerStatusV2 oldTrigger) {
    TriggerKey oldTriggerKey = oldTrigger.trigger.getKey();
    JobKey oldTriggerJobKey = oldTrigger.trigger.getJobKey();

    String[] triggerSplits = oldTriggerKey.getName().split(":");
    String[] oldJobNameSplits = oldTriggerJobKey.getName().split(":");

    // Old TriggerKey name has format = namespace:application:type:program:schedule
    // Old JobKey name has format = namespace:application:type:program
    // If TriggerKey and JobKey have the new format, then return
    if (triggerSplits.length == 6 && oldJobNameSplits.length == 5) {
      return false;
    }

    if (triggerSplits.length == 5) {
      // This trigger key has the old format. So replace it with new format.
      // New TriggerKey name has format = namespace:application:version:type:program:schedule
      String newTriggerName = ScheduleUpgradeUtil.getNameWithDefaultVersion(triggerSplits, 2);
      oldTrigger.trigger.setKey(new TriggerKey(newTriggerName, oldTriggerKey.getGroup()));
    }

    if (oldJobNameSplits.length == 4) {
      // This job key has the old format. So replace it with new format.
      // New JobKey name has format = namespace:application:version:type:program
      String newJobName = ScheduleUpgradeUtil.getNameWithDefaultVersion(oldJobNameSplits, 2);
      oldTrigger.trigger.setJobKey(new JobKey(newJobName, oldTriggerJobKey.getGroup()));
    } else {
      LOG.error("TriggerKey {} has application version in it but JobKey {} didn't.", oldTrigger.trigger.getKey(),
                oldTrigger.trigger.getJobKey());
    }
    return true;
  }

  private void upgradeJobs(Table table) {
    Row result = table.get(JOB_KEY);
    if (result.isEmpty()) {
      return;
    }

    for (byte[] column : result.getColumns().values()) {
      JobDetail oldJobDetail = (JobDetail) SerializationUtils.deserialize(column);
      JobDetail jobDetail = addDefaultAppVersionIfNeeded(oldJobDetail);

      if (!jobDetail.equals(oldJobDetail)) {
        // Write the new jobDetail only if the upgraded key doesn't exist already
        if (readJob(table, jobDetail.getKey()) == null) {
          persistJob(table, jobDetail);
        }
        removeJob(table, oldJobDetail.getKey());
      }
    }
  }

  private void upgradeTriggers(Table table) {
    Row result = table.get(TRIGGER_KEY);
    if (result.isEmpty()) {
      return;
    }

    for (byte[] column : result.getColumns().values()) {
      TriggerStatusV2 triggerStatus = (TriggerStatusV2) SerializationUtils.deserialize(column);
      TriggerKey oldTriggerKey = triggerStatus.trigger.getKey();
      boolean modified = addDefaultAppVersionIfNeeded(triggerStatus);

      if (modified) {
        // Write the new triggerStatus only if the upgraded key doesn't exist already
        if (readTrigger(triggerStatus.trigger.getKey()) == null) {
          persistTrigger(table, triggerStatus.trigger, triggerStatus.state);
        }
        removeTrigger(table, oldTriggerKey);
      }
    }
  }

  /**
   * Trigger and state.
   * New version of TriggerStatus which supports custom serialization from CDAP 3.3 release.
   */
  public static class TriggerStatusV2 implements Externalizable {
    private static final long serialVersionUID = -2972207194129529281L;
    private OperableTrigger trigger;
    private Trigger.TriggerState state;

    private TriggerStatusV2(OperableTrigger trigger, Trigger.TriggerState state) {
      this.trigger = trigger;
      this.state = state;
    }

    public TriggerStatusV2() {
      // no-op
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(trigger);
      out.writeUTF(state.toString());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      trigger = (OperableTrigger) in.readObject();
      state = Trigger.TriggerState.valueOf(in.readUTF());
    }
  }
}
