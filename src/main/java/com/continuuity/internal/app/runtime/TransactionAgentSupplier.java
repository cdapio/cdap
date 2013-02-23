package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.google.common.base.Supplier;

/**
 *
 */
public interface TransactionAgentSupplier {

  DataSetContext getDataSetContext();

  TransactionAgent createAndUpdateProxy();

  TransactionAgent create();
}
