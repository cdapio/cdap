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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.proto.Notification;

import java.util.List;

/**
 * Simple implementation of {@link Job}.
 */
public final class SimpleJob implements Job {
  private final ProgramSchedule schedule;
  private final long creationTime;
  private final JobKey jobKey;
  private final List<Notification> notifications;
  private final State state;
  private final long scheduleLastUpdatedTime;
  private Long deleteTimeMillis;

  /**
   * @param scheduleLastUpdatedTime the last modification time of the schedule, at the time this job is created.
   *                                This serves as a way to detect whether the schedule was changed later-on, and
   *                                hence, the job would be obsolete in that case.
   */
  public SimpleJob(ProgramSchedule schedule, int generationId, long creationTime, List<Notification> notifications,
                   State state, long scheduleLastUpdatedTime) {
    this.schedule = schedule;
    this.creationTime = creationTime;
    this.jobKey = new JobKey(schedule.getScheduleId(), generationId);
    this.notifications = ImmutableList.copyOf(notifications);
    this.state = state;
    this.scheduleLastUpdatedTime = scheduleLastUpdatedTime;
  }

  @Override
  public ProgramSchedule getSchedule() {
    return schedule;
  }

  @Override
  public int getGenerationId() {
    return jobKey.getGenerationId();
  }

  @Override
  public long getScheduleLastUpdatedTime() {
    return scheduleLastUpdatedTime;
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public List<Notification> getNotifications() {
    return notifications;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public JobKey getJobKey() {
    return jobKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleJob that = (SimpleJob) o;

    return Objects.equal(this.schedule, that.schedule) &&
      Objects.equal(this.creationTime, that.creationTime) &&
      Objects.equal(this.jobKey, that.jobKey) &&
      Objects.equal(this.notifications, that.notifications) &&
      Objects.equal(this.state, that.state) &&
      Objects.equal(this.scheduleLastUpdatedTime, that.scheduleLastUpdatedTime) &&
      Objects.equal(this.deleteTimeMillis, that.deleteTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schedule, creationTime, jobKey, notifications, state, scheduleLastUpdatedTime,
                            deleteTimeMillis);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("schedule", schedule)
      .add("creationTime", creationTime)
      .add("jobKey", jobKey)
      .add("notifications", notifications)
      .add("state", state)
      .add("scheduleLastUpdatedTime", scheduleLastUpdatedTime)
      .add("deleteTimeMillis", deleteTimeMillis)
      .toString();
  }

  @Override
  public boolean isToBeDeleted() {
    return deleteTimeMillis != null;
  }

  @Override
  public Long getDeleteTimeMillis() {
    return deleteTimeMillis;
  }

  void setToBeDeleted(long timestamp) {
    this.deleteTimeMillis = timestamp;
  }
}
