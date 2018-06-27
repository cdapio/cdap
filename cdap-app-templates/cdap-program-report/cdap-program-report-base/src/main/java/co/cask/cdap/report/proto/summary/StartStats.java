/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto.summary;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Represents the statistics of the start time of all program runs.
 */
public class StartStats {
  @Nullable
  private final Long oldest;
  @Nullable
  private final Long newest;

  public StartStats(Long oldest, Long newest) {
    this.oldest = oldest;
    this.newest = newest;
  }

  /**
   * @return the oldest start time of all program runs, or {@code null} if there is no program run
   */
  @Nullable
  public Long getOldest() {
    return oldest;
  }

  /**
   * @return the newest start time of all program runs, or {@code null} if there is no program run
   */
  @Nullable
  public Long getNewest() {
    return newest;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StartStats that = (StartStats) o;
    return Objects.equal(oldest, that.oldest) &&
      Objects.equal(newest, that.newest);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(oldest, newest);
  }
}
