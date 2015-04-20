/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.logging.kafka.KafkaLogEvent;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Abstract Log Processor that stores offsets for paritions and checks if a given event is already processed.
 */
public abstract class AbstractKafkaLogProcessor implements KafkaLogProcessor {

  private final Map<Integer, Long> partitionOffsets;
  public AbstractKafkaLogProcessor() {
    this.partitionOffsets = Maps.newHashMap();
  }

  public void init(Set<Integer> partitions, CheckpointManager checkpointManager) {
    partitionOffsets.clear();
    try {
     for (Integer partition : partitions) {
        partitionOffsets.put(partition, checkpointManager.getCheckpoint(partition));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void process(KafkaLogEvent event) {
    if (!alreadyProcessed(event)) {
      doProcess(event);
    }
  }

  /**
   * doProcess method will be called if the event is not already processed.
   *
   * @param event KafkaLogEvent
   */
  protected abstract void doProcess(KafkaLogEvent event);

  public boolean alreadyProcessed(KafkaLogEvent event) {
    // If the offset is -1 then it is not processed.
    // if the event offset is less than what is already checkpointed then the event is already processed
    return event.getNextOffset() != -1 && event.getNextOffset() < partitionOffsets.get(event.getPartition()) ?
           true :
           false;
  }
}
