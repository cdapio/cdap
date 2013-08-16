/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * Represents storage of consumer state.
 *
 * Row key is queueName + dequeue_strategy + groupId + groupSize.
 * If any reconfiguration happened, the row key would changed, causing it to start scanning from
 * some older transaction id (potentially 0) when consuming.
 */
final class HBaseConsumerState {

  private static final byte[] COLUMN_FAMILY = new byte[] { 's' };

  private final HTable hTable;

  HBaseConsumerState(HBaseAdmin admin, String tableName) throws IOException {
    HBaseQueueUtils.createTableIfNotExists(admin, tableName, HBaseQueueConstants.COLUMN_FAMILY,
                                           HBaseQueueConstants.MAX_CREATE_TABLE_WAIT);
    hTable = new HTable(admin.getConfiguration(), tableName);
  }

  /**
   * Returns the transaction id where consumption should starts from.
   *
   * @param queueName Name of the queue.
   * @param consumerConfig Configuration of the consumer.
   * @return The transaction id.
   */
  long getStartTransaction(QueueName queueName, ConsumerConfig consumerConfig) throws IOException {

    byte[] rowKey = Bytes.concat(queueName.toBytes(),
                                 Bytes.toBytes(consumerConfig.getDequeueStrategy().name()),
                                 Bytes.toBytes(consumerConfig.getGroupId()),
                                 Bytes.toBytes(consumerConfig.getGroupSize()));

    Get get = new Get(rowKey);
    get.addColumn(COLUMN_FAMILY, Bytes.toBytes(consumerConfig.getInstanceId()));

    Result result = hTable.get(get);
    if (result.isEmpty()) {
      return 0L;
    }

    KeyValue keyValue = result.getColumnLatest(COLUMN_FAMILY, Bytes.toBytes(consumerConfig.getInstanceId()));
    if (keyValue == null) {
      return 0L;
    }
    return Bytes.toLong(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength());
  }
}
