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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionKey;

/**
 * Represents a {@link Partition} available for consuming.
 */
public interface ConsumablePartition {
  /**
   * @return the PartitionKey associated with this partition
   */
  PartitionKey getPartitionKey();

  /**
   * @return the number of failure attempts this partition has had
   */
  int getNumFailures();

  /**
   * @return the number of failures after incrementing
   */
  int incrementNumFailures();

  /**
   * @return this ConsumablePartition's ProcessState
   */
  ProcessState getProcessState();

  /**
   * Sets a ProcessState to this ConsumablePartition
   */
  void setProcessState(ProcessState processState);

  /**
   * Marks the ProcessState as IN_PROGRESS
   */
  void take();

  /**
   * Marks the ProcessState as AVAILABLE, after it has been taken.
   */
  void untake();

  /**
   * Marks the ProcessState as AVAILABLE, resets the timestamp to 0, and increments the number of failures by 1
   */
  void retry();

  /**
   * Marks the ProcessState as COMPLETED
   */
  void complete();

  /**
   * Marks the ProcessState as DISCARDED
   */
  void discard();

  /**
   * @return the timestamp that this partition was claimed for processing, or 0 if it is not IN_PROGRESS
   */
  long getTimestamp();

  /**
   * Set a timestamp on this partition, when this partition is claimed for processing.
   */
  void setTimestamp(long timestamp);
}
