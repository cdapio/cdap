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

  private final transient String oldestMetricsTimestampMetricName;
  private final transient String latestMetricsTimestampMetricName;


  public TopicProcessMeta(byte[] messageId, long oldestMetricsTimestamp,
                          long latestMetricsTimestamp, long messagesProcessed) {
    this(messageId, oldestMetricsTimestamp, latestMetricsTimestamp, messagesProcessed, null, null);
  }

  public TopicProcessMeta(byte[] messageId, long oldestMetricsTimestamp,
                          long latestMetricsTimestamp, long messagesProcessed,
                          @Nullable String oldestMetricsTimestampMetricName,
                          @Nullable String latestMetricsTimestampMetricName) {
    this.messageId = messageId;
    this.oldestMetricsTimestamp = oldestMetricsTimestamp;
    this.latestMetricsTimestamp = latestMetricsTimestamp;
    this.messagesProcessed = messagesProcessed;
    this.oldestMetricsTimestampMetricName = oldestMetricsTimestampMetricName;
    this.latestMetricsTimestampMetricName = latestMetricsTimestampMetricName;
  }

  void updateStats(byte[] messageId, long timestamp) {
    this.messageId = messageId;
    if (oldestMetricsTimestamp == null || timestamp < oldestMetricsTimestamp) {
      oldestMetricsTimestamp = timestamp;
    }
    if (latestMetricsTimestamp == null || timestamp > latestMetricsTimestamp) {
      latestMetricsTimestamp = timestamp;
    }
    messagesProcessed += 1;
  }

  @Nullable
  public String getOldestMetricsTimestampMetricName() {
    return oldestMetricsTimestampMetricName;
  }

  @Nullable
  public String getLatestMetricsTimestampMetricName() {
    return latestMetricsTimestampMetricName;
  }


  /**
   * resets the timestamp and messages processed, we don't want to reset the messageId.
   */
  void resetMetaInfo() {
    this.oldestMetricsTimestamp = null;
    this.latestMetricsTimestamp = null;
    this.messagesProcessed = 0L;
  }

  @Nullable
  public byte[] getMessageId() {
    return messageId;
  }

  /**
   * get the publish timestamp in seconds from the tms message id
   * @return publish timestamp in seconds ; -1 if null
   */
  public long getPublishTimestamp() {
    return messageId == null ? -1 : TimeUnit.MILLISECONDS.toSeconds(new MessageId(messageId).getPublishTimestamp());
  }

  @Nullable
  public Long getOldestMetricsTimestamp() {
    return oldestMetricsTimestamp;
  }

  @Nullable
  public Long getLatestMetricsTimestamp() {
    return latestMetricsTimestamp;
  }

  public long getMessagesProcessed() {
    return messagesProcessed;
  }

  @Override
  public int hashCode() {
    return Objects.hash(messageId, oldestMetricsTimestamp, latestMetricsTimestamp, messagesProcessed);
  }

  @Override
  public String toString() {
    return "PersistMetaInfo{" +
      "messageId=" + new MessageId(messageId) +
      ", oldestMetricsTimestamp=" + oldestMetricsTimestamp +
      ", latestMetricsTimestamp=" + latestMetricsTimestamp +
      ", messagesProcessed=" + messagesProcessed +
      '}';
  }
}
