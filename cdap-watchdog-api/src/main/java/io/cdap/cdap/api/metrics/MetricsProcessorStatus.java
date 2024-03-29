/*
 * Copyright 2017 Cask Data, Inc.
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
package io.cdap.cdap.api.metrics;

/**
 * Metrics processor status for a topic
 */
public class MetricsProcessorStatus {

  private final MetricsMessageId messageId;
  private final long oldestMetricsTimestamp;
  private final long latestMetricsTimestamp;
  private final long messagesProcessed;
  private final long lastProcessedTimestamp;

  public MetricsProcessorStatus(MetricsMessageId messageId, long oldestMetricsTimestamp,
      long latestMetricsTimestamp, long messagesProcessed, long lastProcessedTimestamp) {
    this.messageId = messageId;
    this.oldestMetricsTimestamp = oldestMetricsTimestamp;
    this.latestMetricsTimestamp = latestMetricsTimestamp;
    this.lastProcessedTimestamp = lastProcessedTimestamp;
    this.messagesProcessed = messagesProcessed;
  }
}
