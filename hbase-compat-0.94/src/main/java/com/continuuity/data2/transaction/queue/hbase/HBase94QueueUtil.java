package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import org.apache.hadoop.hbase.client.HTable;

/**
 * HBase 0.94 implementation of {@link HBaseQueueUtil}.
 */
public class HBase94QueueUtil extends HBaseQueueUtil {
  @Override
  public HBaseQueue2Consumer getQueueConsumer(ConsumerConfig consumerConfig, HTable hTable, QueueName queueName,
                                              HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore) {
    return new HBase94Queue2Consumer(consumerConfig, hTable, queueName, consumerState, stateStore);
  }
}
