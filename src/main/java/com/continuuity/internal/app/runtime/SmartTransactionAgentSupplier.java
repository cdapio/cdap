package com.continuuity.internal.app.runtime;

import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;

/**
 * A {@link TransactionAgentSupplier} that will create a new {@link SmartTransactionAgent} every time
 * when the {@link #get} method is called. Also the newly created {@link TransactionAgent} would be set
 * into the given {@link TransactionProxy} instance.
 */
public final class SmartTransactionAgentSupplier implements TransactionAgentSupplier {

  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final TransactionProxy transactionProxy;

  public SmartTransactionAgentSupplier(OperationExecutor opex,
                                       OperationContext operationCtx,
                                       TransactionProxy transactionProxy) {
    this.opex = opex;
    this.operationCtx = operationCtx;
    this.transactionProxy = transactionProxy;
  }

  @Override
  public TransactionAgent get() {
    TransactionAgent agent = new SmartTransactionAgent(opex, operationCtx);
    transactionProxy.setTransactionAgent(agent);
    return agent;
  }
}
