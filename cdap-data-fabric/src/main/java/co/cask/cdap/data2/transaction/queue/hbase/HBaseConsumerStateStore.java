/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Class for persisting consumer state per queue consumer.
 */
final class HBaseConsumerStateStore {

  private final QueueName queueName;
  private final ConsumerConfig consumerConfig;
  private final HTable hTable;

  HBaseConsumerStateStore(QueueName queueName, ConsumerConfig consumerConfig, HTable hTable) {
    this.queueName = queueName;
    this.consumerConfig = consumerConfig;
    this.hTable = hTable;
  }

  /**
   * Returns the start row as stored in the state store.
   */
  public HBaseConsumerState getState() throws IOException {
    Get get = new Get(queueName.toBytes());
    byte[] column = HBaseQueueAdmin.getConsumerStateColumn(consumerConfig.getGroupId(), consumerConfig.getInstanceId());
    get.addColumn(QueueEntryRow.COLUMN_FAMILY, column);

    return new HBaseConsumerState(hTable.get(get), consumerConfig.getGroupId(), consumerConfig.getInstanceId());
  }

  public void saveState(HBaseConsumerState state) throws IOException {
    // Writes latest startRow to queue config.
    hTable.put(state.updatePut(new Put(queueName.toBytes())));
    hTable.flushCommits();
  }
}
