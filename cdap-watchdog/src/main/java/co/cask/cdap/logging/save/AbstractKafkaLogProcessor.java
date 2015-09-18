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

  private final Map<Integer, Checkpoint> partitonCheckpoints;
  public AbstractKafkaLogProcessor() {
    this.partitonCheckpoints = Maps.newHashMap();
  }

  public void init(Set<Integer> partitions, CheckpointManager checkpointManager) {
    partitonCheckpoints.clear();
    try {
      Map<Integer, Checkpoint> partitionMap = checkpointManager.getCheckpoint(partitions);
      for (Map.Entry<Integer, Checkpoint> partition : partitionMap.entrySet()) {
        partitonCheckpoints.put(partition.getKey(), partition.getValue());
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
    // If no checkpoint is found, then the event needs to be processed.
    // if the event offset is less than or equal to what is already checkpointed then the event is already processed
    Checkpoint checkpoint = partitonCheckpoints.get(event.getPartition());
    return checkpoint != null && event.getNextOffset() <= checkpoint.getNextOffset();
  }
}
