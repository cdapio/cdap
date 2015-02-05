/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.store.timeseries;

import com.google.common.base.Objects;

/**
 * Represents a value of the measure at specific timestamp.
 */
//todo: move in higher-level package? It is used everywhere
public final class TimeValue implements Comparable<TimeValue> {
  private final long timestamp;
  private final long value;

  public TimeValue(long timestamp, long value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    TimeValue other = (TimeValue) obj;

    return timestamp == other.timestamp && value == other.value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("ts", timestamp).add("value", value).toString();
  }

  @Override
  public int compareTo(TimeValue o) {
    return timestamp > o.timestamp ? 1 : (timestamp < o.timestamp ? -1 : 0);
  }
}
