package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * A {@link TransactionAgentSupplier} that will create a new {@link SmartTransactionAgent} every time
 * when the {@link #createAndUpdateProxy} method is called. Also the newly created {@link TransactionAgent} would be set
 * into the given {@link TransactionProxy} instance.
 */
public final class SmartTransactionAgentSupplier implements TransactionAgentSupplier {

  private final OperationExecutor opex;
  private final Program program;
  private final TransactionProxy transactionProxy;

  @Inject
  public SmartTransactionAgentSupplier(OperationExecutor opex,
                                       TransactionProxy transactionProxy,
                                       @Assisted Program program) {
    this.opex = opex;
    this.program = program;
    this.transactionProxy = transactionProxy;
  }

  @Override
  public TransactionAgent createAndUpdateProxy() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    TransactionAgent agent = new SmartTransactionAgent(opex, ctx);
    transactionProxy.setTransactionAgent(agent);
    return agent;
  }

  @Override
  public TransactionAgent create() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    return new SmartTransactionAgent(opex, ctx);
  }
}
