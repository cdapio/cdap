package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.data.operation.OperationContext;

/**
 *
 */
public interface TransactionAgentSupplierFactory {

  TransactionAgentSupplier create(Program program);
}
