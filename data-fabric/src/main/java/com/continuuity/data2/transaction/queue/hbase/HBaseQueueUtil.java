package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import org.apache.hadoop.hbase.client.HTable;

/**
 *
 */
public abstract class HBaseQueueUtil {
  public abstract HBaseQueue2Consumer getQueueConsumer(ConsumerConfig consumerConfig, HTable hTable,
      QueueName queueName, HBaseConsumerState consumerState, HBaseConsumerStateStore stateStore);
}
