package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides default implementation for some of the TransactionAgent methods.
 */
public abstract class AbstractTransactionAgent implements TransactionAgent {

  // this counts successful operations
  private final AtomicInteger succeeded = new AtomicInteger(0);
  // this counts failed operations
  private final AtomicInteger failed = new AtomicInteger(0);

  // the actual operation executor
  protected final OperationExecutor opex;
  // the operation context for all operations
  protected final OperationContext context;

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public AbstractTransactionAgent(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public void start() throws OperationException {
    succeeded.set(0);
    failed.set(0);
  }

  @Override
  public void abort() throws OperationException {
    // TODO auto generated body
  }

  @Override
  public void finish() throws OperationException {
    // TODO auto generated body
  }

  @Override
  public void flush() throws OperationException {
    // DO NOTHING BY DEFAULT
  }

  @Override
  public int getSucceededCount() {
    return succeeded.get();
  }

  @Override
  public int getFailedCount() {
    return failed.get();
  }

  /**
   * Add 1 to the number of succeeded operations.
   */
  protected void succeededOne() {
    succeeded.incrementAndGet();
  }

  /**
   * Add a delta to the number of succeeded operations.
   * @param count how many operations succeeded
   */
  protected void succeededSome(int count) {
    succeeded.addAndGet(count);
  }

  /**
   * Add 1 to the number of failed operations.
   */
  protected void failedOne() {
    failed.incrementAndGet();
  }

  /**
   * Add a delta to the number of failed operations.
   * @param count how many operations failed
   */
  protected void failedSome(int count) {
    failed.addAndGet(count);
  }
}
