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

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ScheduleId;

import javax.annotation.Nullable;

/**
 * Responsible for keeping track of {@link Job}s, which correspond to schedules that have been triggered,
 * but not yet executed.
 */
public interface JobQueue {

  /**
   * Returns a {@link CloseableIterator} over all the jobs associated with the given schedule Id.
   */
  CloseableIterator<Job> getJobsForSchedule(ScheduleId scheduleId);

  /**
   * Returns a stored Job, given the scheduleId and the creationTime of it.
   *
   * @param jobKey the key for the requested Job
   * @return the stored Job, or null if there is no Job for the given key.
   */
  @Nullable
  Job getJob(JobKey jobKey);

  /**
   * Creates a new Job in the queue or updates an existing Job.
   *
   * @param job the new job
   */
  void put(Job job);

  /**
   * Updates a given Job to have a new {@link Job.State}.
   *
   * @param job the job to update the state of
   * @param state the new job state
   * @return a new, updated Job
   * @throws IllegalArgumentException In the case of an illegal state transition. See {@link Job.State}.
   */
  Job transitState(Job job, Job.State state) throws IllegalArgumentException;

  /**
   * Adds the given notification to jobs for the given schedule.
   *
   * @param schedule the schedule for which jobs will be update
   * @param notification the new notification to update the schedule jobs with
   */
  void addNotification(ProgramScheduleRecord schedule, Notification notification);

  /**
   * Marks all jobs associated with the given schedule Id for deletion, recording the time of deletion.
   *
   * @param scheduleId the scheduledId for which to delete
   * @param deletedTime the timestamp to use for the delete marker
   */
  void markJobsForDeletion(ScheduleId scheduleId, long deletedTime);

  /**
   * Deletes the job associated with the given schedule Id and timestamp.
   *
   * @param job the job to delete
   */
  void deleteJob(Job job);

  /**
   * @return the number of partitions in the JobQueue
   */
  int getNumPartitions();

  /**
   * @param partition the partition of the JobQueue to get Jobs from
   * @param lastJobProcessed the job to start the scan from (exclusive), or null to indicate scanning from the start
   * @return A {@link CloseableIterator} over all the jobs in the given partition of the JobQueue
   */
  CloseableIterator<Job> getJobs(int partition, @Nullable Job lastJobProcessed);
}
