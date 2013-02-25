package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;

/**
 *
 */
public interface TransactionAgentSupplierFactory {

  TransactionAgentSupplier create(Program program);
}
