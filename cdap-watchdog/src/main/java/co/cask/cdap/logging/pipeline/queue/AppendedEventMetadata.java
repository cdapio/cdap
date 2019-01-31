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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds information about metadata of appended events such as max appended event timestamp per partition, min/max
 * delay in processing and number of events.
 */
public class AppendedEventMetadata<Offset> {
  private final int eventsAppended;
  private final Map<Integer, CheckpointMetadata<Offset>> checkpointMetadata;

  public AppendedEventMetadata(int eventsAppended,
                               @Nullable Map<Integer, CheckpointMetadata<Offset>> checkpointMetadata) {
    this.eventsAppended = eventsAppended;
    this.checkpointMetadata = checkpointMetadata;
  }

  public int getEventsAppended() {
    return eventsAppended;
  }

  @Nullable
  public Map<Integer, CheckpointMetadata<Offset>> getCheckpointMetadata() {
    return checkpointMetadata;
  }
}
