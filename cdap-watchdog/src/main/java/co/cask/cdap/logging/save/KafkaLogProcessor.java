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

import java.util.Set;

/**
 * Process {@link KafkaLogEvent} with KafkaLogProcessor. init and stop are lifecycle methods that can be called
 * multiple times when kafka leader partitions change.
 */
public interface KafkaLogProcessor {

  /**
   * Called when the leader partitions change.
   * @param partitions new leader partitions.
   */
  void init(Set<Integer> partitions);

  /**
   * Process method will be called for each event received from Kafka from the topics published for log saver.
   *
   * @param event instance of {@link KafkaLogEvent}
   */
  void process(KafkaLogEvent event);

  /**
   * Called to stop processing for the current set of kafka partitions. Stop can be called before init.
   */
  void stop();


  /**
   * Get the checkpoint offset for a given partition. This will be used to figure out the lowest offset to read
   * from for any given partition across multiple plugins. If the plugin doesn't care about check pointing offset
   * the implementations can just return -1;
   *
   * @param partition partition number in kafka
   * @return checkpoint offset
   */
 Checkpoint getCheckpoint(int partition);
}
