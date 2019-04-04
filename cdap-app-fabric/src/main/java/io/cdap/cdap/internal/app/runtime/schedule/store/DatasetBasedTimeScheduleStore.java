/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.commons.lang.SerializationUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * ScheduleStore extends from RAMJobStore and persists the trigger and schedule information into datasets.
 */
public class DatasetBasedTimeScheduleStore extends RAMJobStore {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetBasedTimeScheduleStore.class);
  private static final String JOB_KEY = "job";
  private static final String TRIGGER_KEY = "trigger";

  private final TransactionRunner transactionRunner;

  private final CConfiguration cConf;

  @Inject
  DatasetBasedTimeScheduleStore(TransactionRunner transactionRunner, CConfiguration cConf) {
    this.transactionRunner = transactionRunner;
    this.cConf = cConf;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler) {
    super.initialize(loadHelper, schedSignaler);
    try {
      // See CDAP-7116
      setMisfireThreshold(cConf.getLong(Constants.Scheduler.CFG_SCHEDULER_MISFIRE_THRESHOLD_MS));
      readSchedulesFromPersistentStore();
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
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

  @VisibleForTesting
  static StructuredTable getTimeScheduleStructuredTable(StructuredTableContext context) {
    return context.getTable(StoreDefinition.TimeScheduleStore.SCHEDULES);
  }

  private void executeDelete(final TriggerKey triggerKey) {
    try {
      TransactionRunners.run(transactionRunner, context -> {
        delete(getTimeScheduleStructuredTable(context), TRIGGER_KEY, triggerKey.getName());
      });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void executeDelete(final JobKey jobKey) {
    try {
      TransactionRunners.run(transactionRunner, context -> {
        delete(getTimeScheduleStructuredTable(context), JOB_KEY, jobKey.getName());
      });
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private void persistChangeOfState(final TriggerKey triggerKey, final Trigger.TriggerState newTriggerState) {
    try {
      Preconditions.checkNotNull(triggerKey);
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = getTimeScheduleStructuredTable(context);
        TriggerStatusV2 storedTriggerStatus = readTrigger(table, triggerKey);
        if (storedTriggerStatus != null) {
          // its okay to persist the same trigger back again since during pause/resume
          // operation the trigger does not change. We persist it here with just the new trigger state
          persistTrigger(table, storedTriggerStatus.trigger, newTriggerState);
        } else {
          LOG.warn("Trigger key {} was not found while trying to persist its state to {}.",
                   triggerKey, newTriggerState);
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
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = getTimeScheduleStructuredTable(context);
        if (newJob != null) {
          persistJob(table, newJob);
          LOG.debug("Schedule: stored job with key {}", newJob.getKey());
        }
        if (newTrigger != null) {
          persistTrigger(table, newTrigger, finalTriggerState);
          LOG.debug("Schedule: stored trigger with key {}", newTrigger.getKey());
        }
      });
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  // Persist the job information to dataset
  private void persistJob(StructuredTable table, JobDetail job) throws IOException {
    upsert(table, JOB_KEY, job.getKey().getName(), SerializationUtils.serialize(job));
  }

  private void delete(StructuredTable table, String type, String name) throws IOException {
    table.delete(getPrimaryKeys(type, name));
  }


  @Nullable
  private byte[] get(StructuredTable table, String type, String name) throws IOException {
    Optional<StructuredRow> row = table.read(getPrimaryKeys(type, name));
    if (!row.isPresent()) {
      return null;
    }
    return row.get().getBytes(StoreDefinition.TimeScheduleStore.VALUE_FIELD);
  }

  private List<Field<?>> getPrimaryKeys(String type, String name) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.TimeScheduleStore.TYPE_FIELD, type));
    fields.add(Fields.stringField(StoreDefinition.TimeScheduleStore.NAME_FIELD, name));
    return fields;
  }

  @VisibleForTesting
  @Nullable
  TriggerStatusV2 readTrigger(StructuredTable table, TriggerKey key) throws IOException {
    byte [] result = get(table, TRIGGER_KEY, key.getName());
    if (result != null) {
      return (TriggerStatusV2) SerializationUtils.deserialize(result);
    } else {
      return null;
    }
  }

  private void upsert(StructuredTable table, String type, String name, byte[] data) throws IOException {
    List<Field<?>> fields = getPrimaryKeys(type, name);
    fields.add(Fields.bytesField(StoreDefinition.TimeScheduleStore.VALUE_FIELD, data));
    table.upsert(fields);
  }

  private void persistTrigger(StructuredTable table, OperableTrigger trigger,
                              Trigger.TriggerState state) throws IOException {
    byte[] data = SerializationUtils.serialize(new TriggerStatusV2(trigger, state));
    upsert(table, TRIGGER_KEY, trigger.getKey().getName(), data);
  }

  private List<Field<?>> getScanPrefix(String type) {
    return ImmutableList.of(Fields.stringField(StoreDefinition.TimeScheduleStore.TYPE_FIELD, type));
  }

  // Get schedule information from persistent store
  private void readSchedulesFromPersistentStore() throws Exception {
    final List<JobDetail> jobs = Lists.newArrayList();
    final List<TriggerStatusV2> triggers = Lists.newArrayList();

    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = getTimeScheduleStructuredTable(context);
      try (CloseableIterator<StructuredRow> iterator =
        table.scan(Range.singleton(getScanPrefix(JOB_KEY)), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          JobDetail jobDetail =
            (JobDetail) SerializationUtils.deserialize(
              iterator.next().getBytes(StoreDefinition.TimeScheduleStore.VALUE_FIELD));
          LOG.debug("Schedule: Job with key {} found", jobDetail.getKey());
          jobs.add(jobDetail);
        }
      }

      try (CloseableIterator<StructuredRow> iterator =
        table.scan(Range.singleton(getScanPrefix(TRIGGER_KEY)), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          TriggerStatusV2 trigger =
            (TriggerStatusV2) SerializationUtils.deserialize(
              iterator.next().getBytes(StoreDefinition.TimeScheduleStore.VALUE_FIELD));
          if (trigger.state.equals(Trigger.TriggerState.NORMAL) ||
            trigger.state.equals(Trigger.TriggerState.PAUSED)) {
            triggers.add(trigger);
            LOG.debug("Schedule: trigger with key {} added", trigger.trigger.getKey());
          } else {
            LOG.debug("Schedule: trigger with key {} and state {} skipped", trigger.trigger.getKey(),
                      trigger.state);
          }
        }
      }
    });

    Set<JobKey> jobKeys = new HashSet<>();
    for (JobDetail job : jobs) {
      super.storeJob(job, true);
      jobKeys.add(job.getKey());
    }

    Set<TriggerKey> triggersWithNoJob = new HashSet<>();
    for (TriggerStatusV2 trigger : triggers) {
      if (!jobKeys.contains(trigger.trigger.getJobKey())) {
        triggersWithNoJob.add(trigger.trigger.getKey());
        continue;
      }
      super.storeTrigger(trigger.trigger, true);
      // if the trigger was paused then pause it back. This is needed because the state of the trigger is not a
      // property associated with the trigger.
      // Its fine to do it this way and we will not run into issues where a triggers get fired before its paused
      // because the scheduler is actually not started at this point.
      if (trigger.state == Trigger.TriggerState.PAUSED) {
        super.pauseTrigger(trigger.trigger.getKey());
      }
    }

    for (TriggerKey key : triggersWithNoJob) {
      LOG.error(String.format("No Job was found for the Trigger key '%s'." +
                                " Deleting the trigger entry from the store.", key));
      executeDelete(key);
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
