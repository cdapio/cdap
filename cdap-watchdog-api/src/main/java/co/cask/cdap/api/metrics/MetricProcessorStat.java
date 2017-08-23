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
 * status about metrics processor
 */
public class MetricProcessorStat {
  String messageId;
  Long messagesProcessed;
  Long oldestTimestamp;
  Long latestTimestamp;

  public MetricProcessorStat(String messageId, long messagesProcessed, long oldestTimestamp, long latestTimestamp) {
    this.messageId = messageId;
    this.messagesProcessed = messagesProcessed;
    this.oldestTimestamp = oldestTimestamp;
    this.latestTimestamp = latestTimestamp;
  }

}
