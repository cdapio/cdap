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

package co.cask.cdap.internal.app.runtime.monitor;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Runtime Monitor consume request
 */
public class MonitorConsumeRequest {
  private final String topic;
  private final String topicConfig;
  @Nullable
  private final String messageId;
  private final boolean inclusive;
  private final int limit;

  public MonitorConsumeRequest(String topic, String topicConfig, @Nullable String messageId,
                               boolean inclusive, int limit) {
    this.topic = topic;
    this.topicConfig = topicConfig;
    this.messageId = messageId;
    this.limit = limit;
    this.inclusive = inclusive;
  }

  public MonitorConsumeRequest(String topic, String topicConfig, @Nullable String messageId, int limit) {
    this(topic, topicConfig, messageId, false, limit);
  }

  public String getTopic() {
    return topic;
  }

  public String getTopicConfig() {
    return topicConfig;
  }

  @Nullable
  public String getMessageId() {
    return messageId;
  }

  public boolean isInclusive() {
    return inclusive;
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

    MonitorConsumeRequest request = (MonitorConsumeRequest) o;

    return inclusive == request.inclusive && limit == request.limit && Objects.equals(topic, request.topic) &&
      Objects.equals(topicConfig, request.topicConfig) && Objects.equals(messageId, request.messageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, topicConfig, messageId, inclusive, limit);
  }

  @Override
  public String toString() {
    return "MonitorConsumeRequest{" +
      "topic='" + topic + '\'' +
      ", topicConfig='" + topicConfig + '\'' +
      ", messageId='" + messageId + '\'' +
      ", inclusive=" + inclusive +
      ", limit=" + limit +
      '}';
  }
}
