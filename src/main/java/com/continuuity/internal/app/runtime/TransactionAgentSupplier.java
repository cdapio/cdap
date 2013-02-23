package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.executor.TransactionAgent;
import com.google.common.base.Supplier;

/**
 *
 */
public interface TransactionAgentSupplier {
  TransactionAgent createAndUpdateProxy();

  TransactionAgent create();
}
