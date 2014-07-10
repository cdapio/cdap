package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import org.apache.hadoop.hbase.client.HTable;

/**
 * HBase 0.96 implementation of {@link HBaseQueueUtil}.
 */
public class HBase96QueueUtil extends HBaseQueueUtil {
  @Override
  public HBaseQueueConsumer getQueueConsumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName,
                                              HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore) {
    return new HBase96QueueConsumer(consumerConfig, hTable, queueName, consumerState, stateStore);
  }
}
