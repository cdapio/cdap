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

/**
 * Represents the statistics of durations of all program runs.
 */
public class DurationStats {
  private final long min;
  private final long max;
  private final double average;

  public DurationStats(long min, long max, double average) {
    this.min = min;
    this.max = max;
    this.average = average;
  }

  /**
   * @return the shortest duration of all program runs
   */
  public long getMin() {
    return min;
  }

  /**
   * @return the longest duration of all program runs
   */
  public long getMax() {
    return max;
  }

  /**
   * @return the average duration of all program runs
   */
  public double getAverage() {
    return average;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DurationStats that = (DurationStats) o;
    return Objects.equal(min, that.min) &&
      Objects.equal(max, that.max) &&
      Objects.equal(average, that.average);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(min, max, average);
  }
}
