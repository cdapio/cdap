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

package co.cask.cdap.logging.pipeline.queue;

import co.cask.cdap.logging.meta.Checkpoint;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds information about metadata of processed log events.
 * @param <Offset> type of the offset stored
 */
public class ProcessedEventMetadata<Offset> {
  private final int totalEventsProcessed;
  private final Map<Integer, Checkpoint<Offset>> checkpoints;

  /**
   * Processed event metadata containing total events processsed and checkpoints for the partitions
   */
  public ProcessedEventMetadata(int totalEventsProcessed, @Nullable Map<Integer, Checkpoint<Offset>> checkpoints) {
    this.totalEventsProcessed = totalEventsProcessed;
    this.checkpoints = checkpoints;
  }

  /**
   * Returns total events processed.
   */
  public int getTotalEventsProcessed() {
    return totalEventsProcessed;
  }

  /**
   * Returns checkpoints for each partition.
   */
  @Nullable
  public Map<Integer, Checkpoint<Offset>> getCheckpoints() {
    return checkpoints;
  }
}
