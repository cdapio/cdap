package com.continuuity.internal.app.runtime;

import com.continuuity.app.queue.QueueName;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.internal.app.queue.QueueConsumerFactory;

/**
 *
 */
public interface TransactionAgentSupplier {

  DataSetContext getDataSetContext();

  TransactionAgent createAndUpdateTransactionAgentProxy();

  TransactionAgent createTransactionAgent();

  QueueConsumerFactory createQueueConsumerFactory(int instanceId,
                                                  long groupId, String groupName,
                                                  QueueConfig queueConfig, QueueName queueName);
}
