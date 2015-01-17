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

package co.cask.cdap.common.stream.notification;

import com.google.common.base.Objects;

/**
 * Notification sent by the stream service when a stream has ingested a certain amount of data,
 * determined in the configuration of the stream.
 */
public class StreamSizeNotification {
  private final long timestamp;
  private final long absoluteSize;

  public StreamSizeNotification(long timestamp, long absoluteSize) {
    this.timestamp = timestamp;
    this.absoluteSize = absoluteSize;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getAbsoluteSize() {
    return absoluteSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamSizeNotification that = (StreamSizeNotification) o;

    return Objects.equal(this.timestamp, that.timestamp) &&
      Objects.equal(this.absoluteSize, that.absoluteSize);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp, absoluteSize);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("absoluteSize", absoluteSize)
      .toString();
  }
}
