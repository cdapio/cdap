package com.continuuity.internal.app.runtime;

import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data2.queue.QueueClientFactory;

/**
 *
 */
public interface DataFabricFacade extends QueueClientFactory {

  DataSetContext getDataSetContext();

  TransactionAgent createAndUpdateTransactionAgentProxy();

  TransactionAgent createTransactionAgent();
}
