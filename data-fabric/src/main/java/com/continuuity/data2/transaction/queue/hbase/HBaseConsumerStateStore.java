/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * Class for persisting consumer state.
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
  public byte[] getStartRow() throws IOException {
    Get get = new Get(queueName.toBytes());
    byte[] column = HBaseQueueUtils.getConsumerStateColumn(consumerConfig.getGroupId(),
                                                                        consumerConfig.getInstanceId());
    get.addColumn(QueueConstants.COLUMN_FAMILY, column);

    Result result = hTable.get(get);
    KeyValue keyValue = result.getColumnLatest(QueueConstants.COLUMN_FAMILY, column);
    return (keyValue == null || keyValue.isEmptyColumn()) ? Bytes.EMPTY_BYTE_ARRAY : keyValue.getValue();
  }

  public void saveStartRow(byte[] startRow) throws IOException {
    // Writes latest startRow to queue config.
    Put put = new Put(queueName.toBytes());

    put.add(QueueConstants.COLUMN_FAMILY,
            HBaseQueueUtils.getConsumerStateColumn(consumerConfig.getGroupId(), consumerConfig.getInstanceId()),
            startRow);
    hTable.put(put);
    hTable.flushCommits();
  }
}
