/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
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
