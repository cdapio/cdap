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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.base.Objects;

/**
 * Meta data about a program schedule, including its current status and last-updated timestamp.
 */
public class ProgramScheduleMeta {

  private final ProgramScheduleStatus status;
  private final long lastUpdateTime;

  public ProgramScheduleMeta(ProgramScheduleStatus status, long lastUpdated) {
    this.status = status;
    this.lastUpdateTime = lastUpdated;
  }

  public ProgramScheduleStatus getStatus() {
    return status;
  }

  public long getLastUpdateTime() { return lastUpdateTime; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramScheduleMeta that = (ProgramScheduleMeta) o;

    return Objects.equal(this.lastUpdateTime, that.lastUpdateTime) &&
      Objects.equal(this.status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(status, lastUpdateTime);
  }
}
