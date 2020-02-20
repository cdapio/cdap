/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Metadata of a schedule
 */
public class ScheduleMetadata {
  private final String status;
  /**
   * Timestamp of the last update
   */
  private final long lastUpdateTime;

  public ScheduleMetadata(@Nullable String status,
                          @Nullable long lastUpdateTime) {
    this.status = status;
    this.lastUpdateTime = lastUpdateTime;
  }

  @Nullable
  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  @Nullable
  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScheduleMetadata that = (ScheduleMetadata) o;
    return Objects.equals(status, that.status) &&
            Objects.equals(lastUpdateTime, that.lastUpdateTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, lastUpdateTime);
  }

  @Override
  public String toString() {
    return "ScheduleMetadata{" +
            "status='" + status +
            ", lsatUpdateTime='" + lastUpdateTime +
            '}';
  }
}
