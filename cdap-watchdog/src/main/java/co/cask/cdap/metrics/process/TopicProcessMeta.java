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

package co.cask.cdap.metrics.process;

import co.cask.cdap.messaging.data.MessageId;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Metrics processor processing information about messageId, metrics processed count, oldest and latest processed
 * timestamp
 */
public final class TopicProcessMeta {
  private byte[] messageId;
  private long oldestMetricsTimestamp;
  private long latestMetricsTimestamp;
  private long messagesProcessed;
  private long lastProcessedTimestamp;

  private final transient String oldestMetricsTimestampMetricName;
  private final transient String latestMetricsTimestampMetricName;


  public TopicProcessMeta(@Nullable byte[] messageId, long oldestMetricsTimestamp,
                          long latestMetricsTimestamp, long messagesProcessed, long lastProcessedTimestamp) {
    this(messageId, oldestMetricsTimestamp, latestMetricsTimestamp, messagesProcessed, lastProcessedTimestamp,
         null, null);
  }

  public TopicProcessMeta(@Nullable byte[] messageId, long oldestMetricsTimestamp,
                          long latestMetricsTimestamp, long messagesProcessed, long lastProcessedTimestamp,
                          @Nullable String oldestMetricsTimestampMetricName,
                          @Nullable String latestMetricsTimestampMetricName) {
    this.messageId = messageId;
    this.oldestMetricsTimestamp = oldestMetricsTimestamp;
    this.latestMetricsTimestamp = latestMetricsTimestamp;
    this.messagesProcessed = messagesProcessed;
    this.lastProcessedTimestamp = lastProcessedTimestamp;
    this.oldestMetricsTimestampMetricName = oldestMetricsTimestampMetricName;
    this.latestMetricsTimestampMetricName = latestMetricsTimestampMetricName;
  }

  void updateStats(byte[] messageId, long timestamp) {
    this.messageId = messageId;
    if (timestamp < oldestMetricsTimestamp) {
      oldestMetricsTimestamp = timestamp;
    }
    if (timestamp > latestMetricsTimestamp) {
      latestMetricsTimestamp = timestamp;
    }
    messagesProcessed += 1;
  }

  void updateLastProcessedTimestamp() {
    lastProcessedTimestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  @Nullable
  public String getOldestMetricsTimestampMetricName() {
    return oldestMetricsTimestampMetricName;
  }

  @Nullable
  public String getLatestMetricsTimestampMetricName() {
    return latestMetricsTimestampMetricName;
  }

  @Nullable
  public byte[] getMessageId() {
    return messageId;
  }

  public long getOldestMetricsTimestamp() {
    return oldestMetricsTimestamp;
  }

  public long getLastProcessedTimestamp() {
    return lastProcessedTimestamp;
  }

  public long getLatestMetricsTimestamp() {
    return latestMetricsTimestamp;
  }

  public long getMessagesProcessed() {
    return messagesProcessed;
  }

  @Override
  public int hashCode() {
    return Objects.hash(messageId, oldestMetricsTimestamp, latestMetricsTimestamp, messagesProcessed);
  }

  private String getMessageIdString() {
    if (messageId == null) {
      return "MessageId : null";
    } else {
      MessageId messageId1 = new MessageId(messageId);
      return "MessageId{" +
        "publishTimestamp=" + messageId1.getPublishTimestamp() +
        ", sequenceId=" + messageId1.getSequenceId()  +
        ", writeTimestamp=" + messageId1.getPayloadWriteTimestamp() +
        ", payloadSequenceId=" + messageId1.getPayloadSequenceId() +
        '}';
    }
  }

  @Override
  public String toString() {
    return "TopicProcessMeta{" +
      getMessageIdString() +
      ", oldestMetricsTimestamp=" + oldestMetricsTimestamp +
      ", latestMetricsTimestamp=" + latestMetricsTimestamp +
      ", lastProcessedTimestamp=" + lastProcessedTimestamp +
      ", messagesProcessed=" + messagesProcessed +
      '}';
  }
}
