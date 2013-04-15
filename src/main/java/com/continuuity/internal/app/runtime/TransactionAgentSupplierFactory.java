package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;

/**
 *
 */
public interface TransactionAgentSupplierFactory {

  TransactionAgentSupplier createTransactionAgentSupplierFactory(Program program);
}
