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
import io.cdap.cdap.proto.id.ScheduleId;

/**
 * Uniquely identifies a Job.
 */
public class JobKey {
  private final ScheduleId scheduleId;
  private final int generationId;

  public JobKey(ScheduleId scheduleId, int generationId) {
    this.scheduleId = scheduleId;
    this.generationId = generationId;
  }

  /**
   * @return the scheduleId of the Job for this key.
   */
  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  /**
   * @return the generation Id of the Job for this key.
   */
  public int getGenerationId() {
    return generationId;
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
      Objects.equal(this.generationId, that.generationId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scheduleId, generationId);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("scheduleId", scheduleId)
      .add("generationId", generationId)
      .toString();
  }
}
