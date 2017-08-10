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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.messaging.data.MessageId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Metrics processor processing information about messageId, metrics processed count, oldest and latest processed
 * timestamp
 */
public final class MetricsProcessorStats {
  private byte[] messageId;
  private Long oldestMetricsTimestamp;
  private Long latestMetricsTimestamp;
  private Long messagesProcessed;

  MetricsProcessorStats(byte[] messageId) {
    this(messageId, null, null, 0L);
  }

  MetricsProcessorStats(byte[] messageId, Long oldestMetricsTimestamp,
                        Long latestMetricsTimestamp, Long messagesProcessed) {
    this.messageId = messageId;
    this.oldestMetricsTimestamp = oldestMetricsTimestamp;
    this.latestMetricsTimestamp = latestMetricsTimestamp;
    this.messagesProcessed = messagesProcessed;
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
