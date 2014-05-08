package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.stream.StreamConsumer;

import java.io.IOException;

/**
 *
 */
public interface DataFabricFacade extends QueueClientFactory {

  DataSetContext getDataSetContext();

  TransactionContext createTransactionManager();

  TransactionExecutor createTransactionExecutor();

  StreamConsumer createStreamConsumer(QueueName streamName, ConsumerConfig consumerConfig) throws IOException;
}
