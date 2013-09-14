package com.continuuity.internal.app.runtime;

import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionManager;

/**
 *
 */
public interface DataFabricFacade extends QueueClientFactory {

  DataSetContext getDataSetContext();

  TransactionManager createTransactionManager();
}
