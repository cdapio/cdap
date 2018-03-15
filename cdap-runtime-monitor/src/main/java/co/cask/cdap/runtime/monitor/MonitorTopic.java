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

package co.cask.cdap.runtime.monitor;

/**
 * Information about Topic to be monitored for a runtime
 */
public class MonitorTopic {
  private String topic;
  private long offset;
  private int limit;

  public MonitorTopic(String topic, long offset, int limit) {
    this.topic = topic;
    this.offset = offset;
    this.limit = limit;
  }

  public String getTopic() {
    return topic;
  }

  public long getOffset() {
    return offset;
  }

  public int getLimit() {
    return limit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MonitorTopic that = (MonitorTopic) o;

    if (offset != that.offset) {
      return false;
    }

    if (limit != that.limit) {
      return false;
    }
    return topic != null ? topic.equals(that.topic) : that.topic == null;

  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + limit;
    return result;
  }

  @Override
  public String toString() {
    return "MonitorTopic{" +
      "topic='" + topic + '\'' +
      ", offset=" + offset +
      ", limit=" + limit +
      '}';
  }
}


