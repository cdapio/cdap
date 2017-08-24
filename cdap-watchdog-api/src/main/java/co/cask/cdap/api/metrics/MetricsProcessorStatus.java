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

package co.cask.cdap.api.metrics;

/**
 * status about metrics processor's current processing for a metric topic
 */
public class MetricsProcessorStatus {
  private final long messagePublishTimestamp;
  private final long messagesProcessed;
  private final long oldestTimestamp;
  private final long latestTimestamp;
  private final long lastProcessedTimestamp;

  /**
   *
   * @param messagePublishTimestamp  timestamp in seconds
   *                                 when the message was first published to the messaging system
   * @param messagesProcessed number of messages processed in the most recent iteration of processing
   * @param oldestTimestamp oldest timestamp among the processed metrics
   * @param latestTimestamp latest timestamp among the processed metrics
   * @param lastProcessedTimestamp timestamp when messages was last processed by metrics processor for the topic
   */
  public MetricsProcessorStatus(long messagePublishTimestamp,
                                long messagesProcessed, long oldestTimestamp, long latestTimestamp,
                                long lastProcessedTimestamp) {
    this.messagePublishTimestamp = messagePublishTimestamp;
    this.messagesProcessed = messagesProcessed;
    this.oldestTimestamp = oldestTimestamp;
    this.latestTimestamp = latestTimestamp;
    this.lastProcessedTimestamp = lastProcessedTimestamp;
  }
}
