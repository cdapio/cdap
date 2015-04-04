/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.service.StreamFetchHandler;

/**
 * A {@link ReadFilter} for accepting events that are within a given time range.
 */
public final class TimeRangeReadFilter extends ReadFilter {
  private final long startTime;
  private final long endTime;
  private long hint;
  private boolean active;

  /**
   * Creates a {@link TimeRangeReadFilter} with the specific time range.
   *
   * @param startTime start timestamp for event to be accepted (inclusive)
   * @param endTime end timestamp for event to be accepted (exclusive)
   */
  public TimeRangeReadFilter(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public void reset() {
    hint = -1L;
    active = false;
  }

  @Override
  public long getNextTimestampHint() {
    return hint;
  }

  @Override
  public boolean acceptTimestamp(long timestamp) {
    if (timestamp < startTime) {
      // Reading of stream events is always sorted by timestamp
      // If the timestamp read is still smaller than the start time, there is still chance
      // that there will be events that can satisfy this filter, hence needs to keep reading.
      active = true;
      hint = startTime;
      return false;
    }
    if (timestamp >= endTime) {
      // If the timestamp read already passed the end time, further reading will not get any more events that
      // can satisfy this filter.
      active = false;
      hint = Long.MAX_VALUE;
      return false;
    }
    active = true;
    return true;
  }

  /**
   * Returns true if this filter has been called at least once after the prior call to {@link #reset()}.
   */
  public boolean isActive() {
    return active;
  }
}
