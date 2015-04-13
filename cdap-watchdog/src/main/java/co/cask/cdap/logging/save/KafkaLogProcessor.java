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
import co.cask.cdap.watchdog.election.PartitionChangeHandler;

/**
 * Process {@link KafkaLogEvent} with KafkaLogProcessor. Extends PartitionChangeHandler to implement the necessary
 * changes for partitions changed events.
 */
public interface KafkaLogProcessor extends PartitionChangeHandler {

  /**
   * Process method will be called for each event received from Kafka from the topics published for log saver.
   *
   * @param event instance of {@link KafkaLogEvent}
   */
  public void process(KafkaLogEvent event);

  /**
   * Called to perform cleanup tasks. This method will be called partitionChanged is called as well as during shutdown.
   * method any further.
   */
  public void stop();

}
