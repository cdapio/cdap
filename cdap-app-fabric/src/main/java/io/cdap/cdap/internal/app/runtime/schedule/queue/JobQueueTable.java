/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset that stores {@link Job}s, which correspond to schedules that have been triggered, but not yet executed.
 * The queue can have only one {@link Job.State#PENDING_TRIGGER} job per schedule. It can have other jobs for the
 * same schedule in various other states - marked for deletion, {@link Job.State#PENDING_LAUNCH}, etc. <p/>
 *
 * The queue is designed to avoid conflicts when different services use it concurrently.
 * For instance, when notification processors are adding notifications to a job, the job can be concurrently
 * marked for deletion using the REST API. These two operations should not conflict. The queue handles this by
 * storing the job details and the job deletion information in separate rows. <p/>
 *
 * However when multiple notification processors try to add a notification to a single job, they need to conflict
 * so that there is no lost update. The queue handles this by having a generation id for the jobs. All the notifications
 * are added to the job with the highest generation id, thus creating conflict on concurrent updates.
 * If the job with the highest generation id is marked for deletion, then the generation id is incremented by one
 * and a new job is created, again creating conflicts on concurrent creation of jobs. <p/>
 *
 * Row Key is in the following format for a job: <p/>
 *     &lt;partition_id>&lt;scheduleId>&lt;generationI>&lt;rowType
 * <ul>
 *   <li>The &lt;partition_id> is a hash based upon the scheduleId</li>
 *   <li>The &lt;generationId> is used to distinguish jobs for the same schedule in the queue</li>
 *   <li>The &lt;rowType> is used to record the value stored in that row - job data, marked for deletion time, etc.</li>
 * </ul>
 */
public class JobQueueTable implements JobQueue {

  private static final String TMS_SUBSCRIBER_ID = "job.queue.subscriber";
  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  private final StructuredTable jobQueueTable;
  private final AppMetadataStore appMetadataStore;
  private final int numPartitions;

  JobQueueTable(StructuredTable jobQueueTable, AppMetadataStore appMetadataStore, int numPartitions) {
    this.jobQueueTable = jobQueueTable;
    this.appMetadataStore = appMetadataStore;
    this.numPartitions = numPartitions;
  }

  public static JobQueueTable getJobQueue(StructuredTableContext context, CConfiguration cConf) {
    StructuredTable jobQueueTable = context.getTable(StoreDefinition.JobQueueStore.JOB_QUEUE_TABLE);
    return new JobQueueTable(jobQueueTable, AppMetadataStore.create(context),
                             cConf.getInt(Constants.Scheduler.JOB_QUEUE_NUM_PARTITIONS));
  }

  @Override
  public CloseableIterator<Job> getJobsForSchedule(ScheduleId scheduleId) throws IOException {
    CloseableIterator<StructuredRow> iterator =
      jobQueueTable.scan(Range.singleton(getScheduleScanKeys(scheduleId)), Integer.MAX_VALUE);
    return createJobIterator(iterator);
  }

  @Override
  public Job getJob(JobKey jobKey) throws IOException {
    Range range = Range.singleton(getJobScanKeys(jobKey.getScheduleId(), jobKey.getGenerationId()));
    try (CloseableIterator<Job> iterator =
           createJobIterator(jobQueueTable.scan(range, Integer.MAX_VALUE))) {
      if (iterator.hasNext()) {
        return iterator.next();
      }
      return null;
    }
  }

  void put(Job job) throws IOException {
    writeJob(job);
  }

  @Override
  public Job transitState(Job job, Job.State state) throws IOException {
    // assert that the job state transition is valid
    job.getState().checkTransition(state);
    Job newJob = new SimpleJob(job.getSchedule(), job.getGenerationId(), job.getCreationTime(), job.getNotifications(),
                               state, job.getScheduleLastUpdatedTime());
    writeJob(newJob);
    return newJob;
  }

  @Override
  public void addNotification(ProgramScheduleRecord record, Notification notification) throws IOException {
    boolean jobExists = false;
    ProgramSchedule schedule = record.getSchedule();

    // Only add notifications for enabled schedules
    if (record.getMeta().getStatus() != ProgramScheduleStatus.SCHEDULED) {
      return;
    }

    int nextGenerationId = 0;
    try (CloseableIterator<Job> jobs = getJobsForSchedule(schedule.getScheduleId())) {
      while (jobs.hasNext()) {
        Job job = jobs.next();
        if (job.getGenerationId() >= nextGenerationId) {
          nextGenerationId = job.getGenerationId() + 1;
        }
        if (job.getState() == Job.State.PENDING_TRIGGER) {
          // only update the job's notifications if it is in PENDING_TRIGGER, so as to avoid conflict with the
          // ConstraintCheckerService
          if (job.isToBeDeleted()) {
            // ignore, it will be deleted by ConstraintCheckerService
            continue;
          }
          long scheduleLastUpdated = record.getMeta().getLastUpdateTime();
          if (job.getScheduleLastUpdatedTime() != scheduleLastUpdated) {
            // schedule has changed: this job is obsolete
            writeJobObsolete(job, System.currentTimeMillis());
          } else if (System.currentTimeMillis() - job.getCreationTime() > job.getSchedule().getTimeoutMillis()) {
            // job has timed out; mark it obsolete
            writeJobObsolete(job, System.currentTimeMillis());
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
      writeJob(new SimpleJob(schedule, nextGenerationId, System.currentTimeMillis(), notifications, jobState,
                        record.getMeta().getLastUpdateTime()));
    }
  }

  private void addNotification(Job job, Notification notification) throws IOException {
    List<Notification> notifications = new ArrayList<>(job.getNotifications());
    notifications.add(notification);

    Job.State newState = job.getState();
    if (isTriggerSatisfied(job.getSchedule(), notifications)) {
      newState = Job.State.PENDING_CONSTRAINT;
      job.getState().checkTransition(newState);
    }
    Job newJob = new SimpleJob(job.getSchedule(), job.getGenerationId(), job.getCreationTime(), notifications, newState,
                               job.getScheduleLastUpdatedTime());
    writeJob(newJob);
  }

  private boolean isTriggerSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    return ((SatisfiableTrigger) schedule.getTrigger()).isSatisfied(schedule, notifications);
  }

  @Override
  public void markJobsForDeletion(ScheduleId scheduleId, long markedTime) throws IOException {
    try (CloseableIterator<Job> iterator =
           createJobIterator(jobQueueTable.scan(Range.singleton(getScheduleScanKeys(scheduleId)), Integer.MAX_VALUE))) {
      while (iterator.hasNext()) {
        Job job = iterator.next();
        // only mark jobs that are not marked yet to avoid chance of conflict with concurrent delete
        if (job.getState() != Job.State.PENDING_LAUNCH && !job.isToBeDeleted()) {
          // jobs that are pending launch will be deleted by the launcher anyway
          writeJobDelete(job, markedTime);
        }
      }
    }
  }

  @Override
  public void deleteJob(Job job) throws IOException {
    jobQueueTable.deleteAll(Range.singleton(getJobScanKeys(job.getSchedule().getScheduleId(), job.getGenerationId())));
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  @Override
  public CloseableIterator<Job> getJobs(int partition, @Nullable Job lastJobProcessed) throws IOException {
    Collection<Field<?>> begin;
    Range.Bound beginBound;

    Set<Field<?>> partitionField =
      Collections.singleton(Fields.intField(StoreDefinition.JobQueueStore.PARTITION_ID, partition));
    if (lastJobProcessed == null) {
      begin = partitionField;
      beginBound = Range.Bound.INCLUSIVE;
    } else {
      // sanity check that the specified job is from the same partition
      Preconditions.checkArgument(partition == getPartition(lastJobProcessed.getSchedule().getScheduleId()),
                                  "Job is not from partition '%s': %s", partition, lastJobProcessed);
      begin = getJobScanKeys(lastJobProcessed.getSchedule().getScheduleId(), lastJobProcessed.getGenerationId());
      // we want to exclude the given Job from the scan
      beginBound = Range.Bound.EXCLUSIVE;
    }
    Range range = Range.create(begin, beginBound, partitionField, Range.Bound.INCLUSIVE);
    return createJobIterator(jobQueueTable.scan(range, Integer.MAX_VALUE));
  }

  // full scan of JobQueueTable
  public CloseableIterator<Job> fullScan() throws IOException {
    return createJobIterator(jobQueueTable.scan(Range.all(), Integer.MAX_VALUE));
  }

  private CloseableIterator<Job> createJobIterator(CloseableIterator<StructuredRow> rowIterator) {
    return new AbstractCloseableIterator<Job>() {
      PeekingIterator<StructuredRow> peekingIterator = Iterators.peekingIterator(rowIterator);
      @Override
      protected Job computeNext() {
        if (!peekingIterator.hasNext()) {
          return endOfData();
        }
        return toJob(peekingIterator);
      }

      @Override
      public void close() {
        rowIterator.close();
      }
    };
  }

  /**
   * Construct a Job object from the rows in iterator.
   * A job may need up to three rows from the iterator for its construction -
   * <ul>
   *   <li>Row JOB - the row containing the job data, required</li>
   *   <li>Row DELETE - the row containing the time when the job was marked for deletion, optional</li>
   *   <li>Row OBSOLETE - the row containing the time when the job was marked as obsolete, optional</li>
   * </ul>
   * The above three rows will always be next to each other due to sorting.
   *
   * @param peekingIterator should have at least one element
   * @return a Job object for the first schedule in the iterator
   */
  private Job toJob(PeekingIterator<StructuredRow> peekingIterator) {
    SimpleJob job = null;
    Long toBeDeletedTime = null;
    Long isObsoleteTime = null;

    // Get the schedule id for the job from the first element
    String scheduleId = getScheduleId(peekingIterator.peek());
    // Also get the generationId to only read the rows for the current job
    int generationId = getGenerationId(peekingIterator.peek());
    // Get all the rows for the current job from the iterator
    while (peekingIterator.hasNext() && generationId == getGenerationId(peekingIterator.peek()) &&
      scheduleId.equals(getScheduleId(peekingIterator.peek()))) {
      StructuredRow row = peekingIterator.next();
      StoreDefinition.JobQueueStore.RowType rowType = getRowType(row);
      switch (rowType) {
        case JOB:
          job = fromStructuredRow(row);
          break;
        case DELETE:
          toBeDeletedTime = row.getLong(StoreDefinition.JobQueueStore.DELETE_TIME);
          break;
        case OBSOLETE:
          isObsoleteTime = row.getLong(StoreDefinition.JobQueueStore.OBSOLETE_TIME);
          break;
        default:
          // Should not happen unless a new value is added to the RowType enum
          throw new IllegalStateException(String.format("Unknown row type encountered in job queue: %s", rowType));
      }
    }

    if (job == null) {
      // Should not happen since we always write delete time or obsolete time only after reading the job from store
      throw new IllegalStateException(String.format("Cannot find job for schedule id: %s", scheduleId));
    }

    Long timeToSet = toBeDeletedTime == null ? isObsoleteTime :
      isObsoleteTime == null ? toBeDeletedTime : new Long(Math.min(isObsoleteTime, toBeDeletedTime));
    if (timeToSet != null) {
      job.setToBeDeleted(timeToSet);
    }
    return job;
  }

  private SimpleJob fromStructuredRow(StructuredRow row) {
    return GSON.fromJson(row.getString(StoreDefinition.JobQueueStore.JOB), SimpleJob.class);
  }

  private StoreDefinition.JobQueueStore.RowType getRowType(StructuredRow row) {
    String rowType = row.getString(StoreDefinition.JobQueueStore.ROW_TYPE);
    // the type cannot be null since it is part of the primary key
    return StoreDefinition.JobQueueStore.RowType.valueOf(rowType);
  }

  private int getGenerationId(StructuredRow row) {
    // GenerationId cannot be null since it is part of the primary key
    return row.getInteger(StoreDefinition.JobQueueStore.GENERATION_ID);
  }

  private String getScheduleId(StructuredRow row) {
    return row.getString(StoreDefinition.JobQueueStore.SCHEDULE_ID);
  }

  private void writeJob(Job job) throws IOException {
    Collection<Field<?>> fields = getJobKeys(job.getSchedule().getScheduleId(), job.getGenerationId(),
                                             StoreDefinition.JobQueueStore.RowType.JOB);
    fields.add(Fields.stringField(StoreDefinition.JobQueueStore.JOB, GSON.toJson(job)));
    jobQueueTable.upsert(fields);
    if (job.isToBeDeleted()) {
      writeJobDelete(job, job.getDeleteTimeMillis());
    }
  }

  private void writeJobDelete(Job job, Long deleteTime) throws IOException {
    Collection<Field<?>> fields = getJobKeys(job.getSchedule().getScheduleId(), job.getGenerationId(),
                                             StoreDefinition.JobQueueStore.RowType.DELETE);
    fields.add(Fields.longField(StoreDefinition.JobQueueStore.DELETE_TIME, deleteTime));
    jobQueueTable.upsert(fields);
  }

  private void writeJobObsolete(Job job, long obsoleteTime) throws IOException {
    Collection<Field<?>> fields = getJobKeys(job.getSchedule().getScheduleId(), job.getGenerationId(),
                                             StoreDefinition.JobQueueStore.RowType.OBSOLETE);
    fields.add(Fields.longField(StoreDefinition.JobQueueStore.OBSOLETE_TIME, obsoleteTime));
    jobQueueTable.upsert(fields);
  }

  private Collection<Field<?>> getJobKeys(ScheduleId scheduleId, int generationId,
                                          StoreDefinition.JobQueueStore.RowType rowType) {
    Collection<Field<?>> keys = getJobScanKeys(scheduleId, generationId);
    keys.add(Fields.stringField(StoreDefinition.JobQueueStore.ROW_TYPE, rowType.toString()));
    return keys;
  }

  private Collection<Field<?>> getJobScanKeys(ScheduleId scheduleId, int generationId) {
    Collection<Field<?>> keys = getScheduleScanKeys(scheduleId);
    keys.add(Fields.intField(StoreDefinition.JobQueueStore.GENERATION_ID, generationId));
    return keys;
  }

  private Collection<Field<?>> getScheduleScanKeys(ScheduleId scheduleId) {
    Collection<Field<?>> keys = getPartitionScanKeys(scheduleId);
    keys.add(Fields.stringField(StoreDefinition.JobQueueStore.SCHEDULE_ID,
                                Joiner.on(".").join(scheduleId.toIdParts())));
    return keys;
  }

  private Collection<Field<?>> getPartitionScanKeys(ScheduleId scheduleId) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.intField(StoreDefinition.JobQueueStore.PARTITION_ID, getPartition(scheduleId)));
    return keys;
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
    return Math.abs(hash) % numPartitions;
  }

  /**
   * Gets the id of the last fetched message that was set the given TMS topic
   *
   * @param topic the topic to lookup the last message id
   * @return the id of the last fetched message for this subscriber on this topic,
   *         or {@code null} if no message id was stored before
   */
  // TODO: CDAP-14876 Move this into the new table for subscriber state during re-factoring
  public String retrieveSubscriberState(String topic) throws IOException {
    return appMetadataStore.retrieveSubscriberState(topic, TMS_SUBSCRIBER_ID);
  }

  /**
   * Updates the given topic's last fetched message id with the given message id.
   *
   * @param topic the topic to persist the message id
   * @param messageId the most recently processed message id
   */
  // TODO: CDAP-14876 Move this into the new table for subscriber state during re-factoring
  public void persistSubscriberState(String topic, String messageId) throws IOException {
    appMetadataStore.persistSubscriberState(topic, TMS_SUBSCRIBER_ID, messageId);
  }
}
