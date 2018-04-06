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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AbstractSatisfiableCompositeTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.tephra.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Dataset that stores {@link Job}s, which correspond to schedules that have been triggered, but not yet executed.
 *
 * Row Key is in the following formats:
 *   For Jobs:
 *     'J':<partition_id>:<scheduleId>:<timestamp>
 *     The <partition_id> is a hash based upon the scheduleId
 *
 *   For TMS MessageId:
 *     'M':<topic>
 */
public class JobQueueDataset extends AbstractDataset implements JobQueue {

  static final String EMBEDDED_TABLE_NAME = "t"; // table
  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  // simply serialize the entire Job into one column
  private static final byte[] COL = new byte[] {'C'};
  private static final byte[] TO_DELETE_COL = new byte[] {'D'};
  private static final byte[] IS_OBSOLETE_COL = new byte[] {'O'};
  private static final byte[] JOB_ROW_PREFIX = new byte[] {'J'};
  private static final byte[] ROW_KEY_SEPARATOR = new byte[] {':'};
  private static final byte[] MESSAGE_ID_ROW_PREFIX = new byte[] {'M'};

  private static final int NUM_PARTITIONS = 16;

  private final Table table;
  private final Collection<byte[]> scheduleIds;

  JobQueueDataset(String instanceName, @EmbeddedDataset(EMBEDDED_TABLE_NAME) Table table) {
    super(instanceName, table);
    this.table = table;
    this.scheduleIds = new ArrayList<>();
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    scheduleIds.clear();
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    Collection<byte[]> txChanges = super.getTxChanges();
    if (scheduleIds.isEmpty()) {
      return txChanges;
    }
    Collection<byte[]> totalTxChanges = new ArrayList<>(txChanges.size() + scheduleIds.size());
    totalTxChanges.addAll(txChanges);
    totalTxChanges.addAll(scheduleIds);
    return totalTxChanges;
  }

  @Override
  public CloseableIterator<Job> getJobsForSchedule(ScheduleId scheduleId) {
    byte[] keyPrefix = getRowKeyPrefix(scheduleId);
    return createCloseableIterator(table.scan(keyPrefix, Bytes.stopKeyForPrefix(keyPrefix)));
  }

  @Override
  public Job getJob(JobKey jobKey) {
    Row row = table.get(getRowKey(jobKey.getScheduleId(), jobKey.getCreationTime()));
    if (row.isEmpty()) {
      return null;
    }
    return fromRow(row);
  }

  @Override
  public void put(Job job) {
    table.put(toPut(job));
  }

  @Override
  public Job transitState(Job job, Job.State state) {
    // assert that the job state transition is valid
    job.getState().checkTransition(state);
    Job newJob = new SimpleJob(job.getSchedule(), job.getCreationTime(), job.getNotifications(), state,
                               job.getScheduleLastUpdatedTime());
    put(newJob);
    return newJob;
  }

  @Override
  public void addNotification(ProgramScheduleRecord record, Notification notification) {
    boolean jobExists = false;
    ProgramSchedule schedule = record.getSchedule();

    // Only add notifications for enabled schedules
    if (record.getMeta().getStatus() != ProgramScheduleStatus.SCHEDULED) {
      return;
    }

    // If the schedule contains a composite trigger, there can be concurrent transactions trying to get existing job
    // for this schedule, and they can both create a new job without seeing the new job created by the other
    // transaction. Add the schedule id to the transaction change set so that concurrent transactions of the same
    // schedule can have conflict and retry. This prevents concurrent transactions from creating duplicated jobs
    // for the same schedule.
    if (schedule.getTrigger() instanceof AbstractSatisfiableCompositeTrigger) {
      scheduleIds.add(getRowKeyPrefix(schedule.getScheduleId()));
    }
    try (CloseableIterator<Job> jobs = getJobsForSchedule(schedule.getScheduleId())) {
      while (jobs.hasNext()) {
        Job job = jobs.next();
        if (job.getState() == Job.State.PENDING_TRIGGER) {
          // only update the job's notifications if it is in PENDING_TRIGGER, so as to avoid conflict with the
          // ConstraintCheckerService
          if (job.isToBeDeleted()) {
            // ignore, it will be deleted by ConstraintCheckerService
            continue;
          }
          long scheduleLastUpdated = record.getMeta().getLastUpdated();
          if (job.getScheduleLastUpdatedTime() != scheduleLastUpdated) {
            // schedule has changed: this job is obsolete
            table.put(getRowKey(job.getJobKey().getScheduleId(), job.getJobKey().getCreationTime()),
                      IS_OBSOLETE_COL, Bytes.toBytes(System.currentTimeMillis()));
          } else if (System.currentTimeMillis() - job.getCreationTime() > job.getSchedule().getTimeoutMillis()) {
            // job has timed out; mark it obsolete
            table.put(getRowKey(job.getJobKey().getScheduleId(), job.getJobKey().getCreationTime()),
                      IS_OBSOLETE_COL, Bytes.toBytes(System.currentTimeMillis()));
          } else {
            jobExists = true;
            addNotification(job, notification);
            break;
          }
        }
      }
    }
    // if no job exists for the scheduleId, add a new job with the first notification
    if (!jobExists) {
      List<Notification> notifications = Collections.singletonList(notification);
      Job.State jobState = isTriggerSatisfied(schedule, notifications)
        ? Job.State.PENDING_CONSTRAINT : Job.State.PENDING_TRIGGER;
      put(new SimpleJob(schedule, System.currentTimeMillis(), notifications, jobState,
                        record.getMeta().getLastUpdated()));
    }
  }

  private void addNotification(Job job, Notification notification) {
    List<Notification> notifications = new ArrayList<>(job.getNotifications());
    notifications.add(notification);

    Job.State newState = job.getState();
    if (isTriggerSatisfied(job.getSchedule(), notifications)) {
      newState = Job.State.PENDING_CONSTRAINT;
      job.getState().checkTransition(newState);
    }
    Job newJob = new SimpleJob(job.getSchedule(), job.getCreationTime(), notifications, newState,
                               job.getScheduleLastUpdatedTime());
    put(newJob);
  }

  private boolean isTriggerSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    return ((SatisfiableTrigger) schedule.getTrigger()).isSatisfied(schedule, notifications);
  }

  @Override
  public void markJobsForDeletion(ScheduleId scheduleId, long markedTime) {
    byte[] keyPrefix = getRowKeyPrefix(scheduleId);
    Row row;
    try (Scanner scanner = table.scan(keyPrefix, Bytes.stopKeyForPrefix(keyPrefix))) {
      while ((row = scanner.next()) != null) {
        Job job = fromRow(row);
        // only mark jobs that are not marked yet to avoid chance of conflict with concurrent delete
        if (job.getState() != Job.State.PENDING_LAUNCH && row.get(TO_DELETE_COL) == null) {
          // jobs that are pending launch will be deleted by the launcher anyway
          table.put(row.getRow(), TO_DELETE_COL, Bytes.toBytes(markedTime));
        }
      }
    }
  }

  @Override
  public void deleteJob(Job job) {
    table.delete(getRowKey(job.getSchedule().getScheduleId(), job.getCreationTime()));
  }

  @Override
  public int getNumPartitions() {
    return NUM_PARTITIONS;
  }

  @Override
  public CloseableIterator<Job> getJobs(int partition, @Nullable Job lastJobProcessed) {
    byte[] jobRowPrefix = getJobRowPrefix(partition);
    byte[] startKey;
    if (lastJobProcessed == null) {
      startKey = jobRowPrefix;
    } else {
      // sanity check that the specified job is from the same partition
      Preconditions.checkArgument(partition == getPartition(lastJobProcessed.getSchedule().getScheduleId()),
                                  "Job is not from partition '%s': %s", partition, lastJobProcessed);
      byte[] jobRowKey = getRowKey(lastJobProcessed.getSchedule().getScheduleId(), lastJobProcessed.getCreationTime());
      // we want to exclude the given Job from the scan
      startKey = Bytes.stopKeyForPrefix(jobRowKey);
    }
    byte[] stopKey = Bytes.stopKeyForPrefix(jobRowPrefix);
    return createCloseableIterator(table.scan(startKey, stopKey));
  }

  // full scan of JobQueueDataset
  public CloseableIterator<Job> fullScan() {
    return createCloseableIterator(table.scan(JOB_ROW_PREFIX, Bytes.stopKeyForPrefix(JOB_ROW_PREFIX)));
  }

  private CloseableIterator<Job> createCloseableIterator(final Scanner scanner) {
    return new AbstractCloseableIterator<Job>() {
      @Override
      protected Job computeNext() {
        Row row = scanner.next();
        if (row == null) {
          return endOfData();
        }
        return fromRow(row);
      }

      @Override
      public void close() {
        scanner.close();
      }
    };
  }

  private Job fromRow(Row row) {
    String jobJsonString = Bytes.toString(row.get(COL));
    SimpleJob job = GSON.fromJson(jobJsonString, SimpleJob.class);
    Long toBeDeletedTime = row.getLong(TO_DELETE_COL);
    Long isObsoleteTime = row.getLong(IS_OBSOLETE_COL);
    Long timeToSet = toBeDeletedTime == null ? isObsoleteTime :
      isObsoleteTime == null ? toBeDeletedTime : new Long(Math.min(isObsoleteTime, toBeDeletedTime));
    if (timeToSet != null) {
      job.setToBeDeleted(timeToSet);
    }
    return job;
  }

  private Put toPut(Job job) {
    ScheduleId scheduleId = job.getSchedule().getScheduleId();
    return new Put(getRowKey(scheduleId, job.getCreationTime()), COL, GSON.toJson(job));
  }

  private byte[] getJobRowPrefix(int bucket) {
    byte[] bucketByte = {((byte) bucket)};
    return Bytes.concat(JOB_ROW_PREFIX, ROW_KEY_SEPARATOR, bucketByte, ROW_KEY_SEPARATOR);
  }

  private byte[] getRowKeyPrefix(ScheduleId scheduleId) {
    byte[] scheduleIdBytes = Bytes.toBytes(Joiner.on(".").join(scheduleId.toIdParts()));
    return Bytes.concat(getJobRowPrefix(getPartition(scheduleId)), scheduleIdBytes, ROW_KEY_SEPARATOR);
  }

  @VisibleForTesting
  int getPartition(ScheduleId scheduleId) {
    // Similar to ScheduleId#hashCode, but that is not consistent across runtimes due to how Enum#hashCode works.
    // Ensure that the hash won't change across runtimes:
    int hash = Hashing.murmur3_32().newHasher()
      .putString(scheduleId.getNamespace())
      .putString(scheduleId.getApplication())
      .putString(scheduleId.getVersion())
      .putString(scheduleId.getSchedule())
      .hash().asInt();
    return Math.abs(hash) % NUM_PARTITIONS;
  }

  private byte[] getRowKey(ScheduleId scheduleId, long timestamp) {
    return Bytes.add(getRowKeyPrefix(scheduleId), Bytes.toBytes(timestamp));
  }

  /**
   * Gets the id of the last fetched message that was set the given TMS topic
   *
   * @param topic the topic to lookup the last message id
   * @return the id of the last fetched message for this subscriber on this topic,
   *         or {@code null} if no message id was stored before
   */
  public String retrieveSubscriberState(String topic) {
    Row row = table.get(getRowKey(topic));
    byte[] messageIdBytes = row.get(COL);
    return messageIdBytes == null ? null : Bytes.toString(messageIdBytes);
  }

  /**
   * Updates the given topic's last fetched message id with the given message id.
   *
   * @param topic the topic to persist the message id
   * @param messageId the most recently processed message id
   */
  public void persistSubscriberState(String topic, String messageId) {
    table.put(getRowKey(topic), COL, Bytes.toBytes(messageId));
  }

  private byte[] getRowKey(String topic) {
    // For backward compatibility, the subscriber is not used
    return Bytes.concat(MESSAGE_ID_ROW_PREFIX, ROW_KEY_SEPARATOR, Bytes.toBytes(topic));
  }
}
