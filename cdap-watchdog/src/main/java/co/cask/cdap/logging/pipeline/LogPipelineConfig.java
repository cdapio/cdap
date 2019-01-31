/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline;

/**
 * Log pipeline config.
 */
public class LogPipelineConfig {
  private final long maxBufferSize;
  private final long eventDelayMillis;
  private final long checkpointIntervalMillis;

  public LogPipelineConfig(long maxBufferSize, long eventDelayMillis, long checkpointIntervalMillis) {
    this.maxBufferSize = maxBufferSize;
    this.eventDelayMillis = eventDelayMillis;
    this.checkpointIntervalMillis = checkpointIntervalMillis;
  }

  public long getMaxBufferSize() {
    return maxBufferSize;
  }

  public long getEventDelayMillis() {
    return eventDelayMillis;
  }

  public long getCheckpointIntervalMillis() {
    return checkpointIntervalMillis;
  }
}
