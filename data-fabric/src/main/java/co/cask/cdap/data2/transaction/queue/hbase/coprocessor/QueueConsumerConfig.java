/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase.coprocessor;

import co.cask.cdap.api.common.Bytes;

import java.util.Map;

/**
 * Holder class for queue consumer configuration information.
 */
public final class QueueConsumerConfig {
  // Map from consumer instance to
  private final Map<ConsumerInstance, byte[]> startRows;
  private final int numGroups;
  private final byte[] smallest;

  public QueueConsumerConfig(Map<ConsumerInstance, byte[]> startRows, int numGroups) {
    this.startRows = startRows;
    this.numGroups = numGroups;

    byte[] smallest = null;
    for (byte[] row : startRows.values()) {
      if (smallest == null || Bytes.compareTo(row, smallest) < 0) {
        smallest = row;
      }
    }
    this.smallest = smallest;
  }

  public byte[] getStartRow(ConsumerInstance consumerInstance) {
    return startRows.get(consumerInstance);
  }

  public int getNumGroups() {
    return numGroups;
  }

  /**
   * Returns the smallest start row among all consumers.
   */
  public byte[] getSmallestStartRow() {
    return smallest;
  }
}
