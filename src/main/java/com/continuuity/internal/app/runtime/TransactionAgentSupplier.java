package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;

/**
 *
 */
public interface TransactionAgentSupplier {

  DataSetContext getDataSetContext();

  TransactionAgent createAndUpdateProxy();

  TransactionAgent create();
}
