package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;

/**
 *
 */
public interface DataFabricFacade extends QueueClientFactory {

  DataSetContext getDataSetContext();

  TransactionContext createTransactionManager();

  TransactionExecutor createTransactionExecutor();
}
