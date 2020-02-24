/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.base.Objects;
import io.cdap.cdap.proto.ScheduleMetadata;

/**
 * Metadata of a program schedule, including its current status and last-updated timestamp.
 */
public class ProgramScheduleMeta {
  private final ProgramScheduleStatus status;
  /**
   * Timestamp of last update
   */
  private final long lastUpdatedTime;

  public ProgramScheduleMeta(ProgramScheduleStatus status, long lastUpdatedTime) {
    this.status = status;
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public ProgramScheduleStatus getStatus() {
    return status;
  }

  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramScheduleMeta that = (ProgramScheduleMeta) o;

    return Objects.equal(this.lastUpdatedTime, that.lastUpdatedTime) &&
      Objects.equal(this.status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(status, lastUpdatedTime);
  }

  public ScheduleMetadata toScheduleMetadata() {
    return new ScheduleMetadata(status.toString(), lastUpdatedTime);
  }

}
