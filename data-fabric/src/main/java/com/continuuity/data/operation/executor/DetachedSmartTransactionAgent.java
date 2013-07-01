package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Same as {@link SmartTransactionAgent} but that operates on a single given transaction. It will never attempt to
 * start new transaction or commit the given one. All operations are guaranteed to be executed using given transaction.
 */
public class DetachedSmartTransactionAgent extends SmartTransactionAgent {
  private static final Logger LOG =
    LoggerFactory.getLogger(DetachedSmartTransactionAgent.class);

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public DetachedSmartTransactionAgent(OperationExecutor opex, OperationContext context, Transaction tx) {
    super(opex, context, tx);
  }

  @Override
  protected void abortTransaction() throws OperationException {
    // DO NOTHING: this agent operates on transaction that is "owned" on higher level
  }

  @Override
  public void finish() throws OperationException {
    // do NOT commit transaction, but only flush buffered ops: this agent operates on transaction that is "owned" on
    // higher level
    super.flush();
  }
}
