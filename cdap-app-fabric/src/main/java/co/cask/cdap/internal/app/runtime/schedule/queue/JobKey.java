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

import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Objects;

/**
 * Uniquely identifies a Job.
 */
public class JobKey {
  private final ScheduleId scheduleId;
  private final long creationTime;

  public JobKey(ScheduleId scheduleId, long creationTime) {
    this.scheduleId = scheduleId;
    this.creationTime = creationTime;
  }

  /**
   * @return the scheduleId of the Job for this key.
   */
  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  /**
   * @return the creation time of the Job for this key.
   */
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JobKey that = (JobKey) o;

    return Objects.equal(this.scheduleId, that.scheduleId) &&
      Objects.equal(this.creationTime, that.creationTime);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scheduleId, creationTime);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("scheduleId", scheduleId)
      .add("creationTime", creationTime)
      .toString();
  }
}
