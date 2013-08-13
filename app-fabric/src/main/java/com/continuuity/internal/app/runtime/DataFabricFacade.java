package com.continuuity.internal.app.runtime;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.queue.QueueConsumerFactory;
import com.continuuity.internal.app.queue.QueueConsumerFactory.QueueInfo;

/**
 *
 */
public interface DataFabricFacade {

  DataSetContext getDataSetContext();

  TransactionAgent createAndUpdateTransactionAgentProxy();

  TransactionAgent createTransactionAgent();

  QueueConsumerFactory createQueueConsumerFactory(int instanceId, long groupId, String groupName, QueueName queueName,
                                                  QueueInfo queueInfo, boolean singleEntry);
}
