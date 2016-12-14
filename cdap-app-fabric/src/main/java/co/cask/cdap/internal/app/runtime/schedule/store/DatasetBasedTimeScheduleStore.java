/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.commons.lang.SerializationUtils;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ScheduleStore extends from RAMJobStore and persists the trigger and schedule information into datasets.
 */
public class DatasetBasedTimeScheduleStore extends RAMJobStore {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedTimeScheduleStore.class);
  private static final byte[] JOB_KEY = Bytes.toBytes("jobs");
  private static final byte[] TRIGGER_KEY = Bytes.toBytes("trigger");

  private final TransactionExecutorFactory factory;
  private final ScheduleStoreTableUtil tableUtil;
  private final CConfiguration cConf;
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
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            removeTrigger(table, triggerKey);
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void executeDelete(final JobKey jobKey) {
    try {
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            removeJob(table, jobKey);
          }
        });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
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

  private void removeTrigger(Table table, TriggerKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.getName());
    table.delete(TRIGGER_KEY, col);
  }


  private void removeJob(Table table, JobKey key) {
    byte[][] col = new byte[1][];
    col[0] = Bytes.toBytes(key.getName());
    table.delete(JOB_KEY, col);
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

  /**
   * Method to add version in JobKey and VersionKey in SchedulerStore.
   *
   * @throws Exception
   */
  public void upgrade() throws Exception {
    // Create table instance.
    initializeScheduleTable();
    factory.createExecutor(ImmutableList.of((TransactionAware) table))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          upgradeJobs();
          upgradeTriggers();
        }
      });
  }

  private void upgradeJobs() throws Exception {
    Row result = table.get(JOB_KEY);

    if (result.isEmpty()) {
      return;
    }

    for (byte[] column : result.getColumns().values()) {
      JobDetail jobDetail = (JobDetail) SerializationUtils.deserialize(column);
      JobKey oldJobKey = jobDetail.getKey();
      String oldJobKeyName = oldJobKey.getName();
      String[] splits = oldJobKeyName.split(":");
      // Old JobKey name has format = namespace:application:type:program
      if (splits.length != 4) {
        LOG.debug("Skip upgrading Job {}. Expected Job key format 'namespace:application:type:program'", oldJobKey);
        continue;
      }

      // New JobKey name has format = namespace:application:version:type:program
      String newJobName = getNameWithVersion(splits);

      JobDetail newJobDetail = jobDetail.getJobBuilder().withIdentity(newJobName, oldJobKey.getGroup()).build();
      persistJob(table, newJobDetail);
      removeJob(table, oldJobKey);
    }
  }

  private void upgradeTriggers() throws Exception {
    Row result = table.get(TRIGGER_KEY);
    if (result.isEmpty()) {
      return;
    }

    for (byte[] column : result.getColumns().values()) {
      TriggerStatusV2 triggerStatus = (TriggerStatusV2) SerializationUtils.deserialize(column);
      TriggerKey oldTriggerKey = triggerStatus.trigger.getKey();
      JobKey oldTriggerJobKey = triggerStatus.trigger.getJobKey();

      String[] splits = oldTriggerKey.getName().split(":");
      // Old TriggerKey name has format = namespace:application:type:program:schedule
      if (splits.length != 5) {
        LOG.debug("Skip upgrading Trigger {}. Expected trigger key " +
                    "format 'namespace:application:type:program:schedule'", oldTriggerKey);
        continue;
      }
      // New TriggerKey name has format = namespace:application:version:type:program:schedule
      String newTriggerName = getNameWithVersion(splits);
      triggerStatus.trigger.setKey(new TriggerKey(newTriggerName, oldTriggerKey.getGroup()));

      String[] oldJobNameSplits = oldTriggerJobKey.getName().split(":");
      // Old JobKey name has format = namespace:application:type:program
      if (oldJobNameSplits.length != 4) {
        LOG.debug("Skip upgrading Trigger {} with Job {}. Expected job key format " +
                    "'namespace:application:type:program'", oldTriggerKey, oldTriggerJobKey);
        continue;
      }

      // New JobKey name has format = namespace:version:application:type:program
      String newJobName = getNameWithVersion(oldJobNameSplits);
      triggerStatus.trigger.setJobKey(new JobKey(newJobName, oldTriggerJobKey.getGroup()));
      persistTrigger(table, triggerStatus.trigger, triggerStatus.state);
      removeTrigger(table, oldTriggerKey);
    }
  }

  private String getNameWithVersion(String[] splits) {
    List<String> splitsList = new ArrayList<>(Arrays.asList(splits));
    splitsList.add(2, ApplicationId.DEFAULT_VERSION);
    return Joiner.on(":").join(splitsList);
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
