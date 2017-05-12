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

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.Notification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Simple implementation of {@link Job}.
 */
public final class SimpleJob implements Job {
  private final ProgramSchedule schedule;
  private final JobKey jobKey;
  private final List<Notification> notifications;
  private final State state;

  public SimpleJob(ProgramSchedule schedule, long creationTime, List<Notification> notifications, State state) {
    this.schedule = schedule;
    this.jobKey = new JobKey(schedule.getScheduleId(), creationTime);
    this.notifications = ImmutableList.copyOf(notifications);
    this.state = state;
  }

  @Override
  public ProgramSchedule getSchedule() {
    return schedule;
  }

  @Override
  public long getCreationTime() {
    return jobKey.getCreationTime();
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
      Objects.equal(this.jobKey, that.jobKey) &&
      Objects.equal(this.notifications, that.notifications) &&
      Objects.equal(this.state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schedule, jobKey, notifications, state);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("schedule", schedule)
      .add("jobKey", jobKey)
      .add("notifications", notifications)
      .add("state", state)
      .toString();
  }
}
