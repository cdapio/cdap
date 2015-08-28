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

package co.cask.cdap.dq;

/**
 * TimestampValue objects map a given timestamp to a list of values
 * @param <T> type of objects in values
 */
public class TimestampValue<T> {
  private final long timestamp;
  private T value;

  public TimestampValue(long timestamp, T value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public T getValue() {
    return value;
  }
}
