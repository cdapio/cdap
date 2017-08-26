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

  /**
   *
   * @param messageId - metrics message id
   * @param oldestMetricsTimestamp - oldest timestamp among the processed metrics
   * @param latestMetricsTimestamp - latest timestamp among the processed metrics
   * @param messagesProcessed - messages processed in an iteration
   * @param lastProcessedTimestamp - timestamp when the most recent update happened
   * @param oldestMetricsTimestampMetricName - metric name used for oldest metrics timestamp - not serialized
   * @param latestMetricsTimestampMetricName - metric name used for latest metrics timestamp - not serialized
   */
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

  /**
   * updates message id to the passed id, updates latest/oldest timestamp based on the passed timestamp
   * @param messageId
   * @param timestamp
   */
  void updateTopicProcessingStats(byte[] messageId, long timestamp) {
    this.messageId = messageId;
    if (timestamp < oldestMetricsTimestamp) {
      oldestMetricsTimestamp = timestamp;
    }
    if (timestamp > latestMetricsTimestamp) {
      latestMetricsTimestamp = timestamp;
    }
    messagesProcessed += 1;
  }

  /**
   * we update the last processed timestamp to the current time in seconds on this call
   */
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
}
