package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.WriteOperation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This is a transaction agent that performs all operations immediately
 * synchronously, each in its own transaction. If an error occurs during
 * an operation, the agent remains operational (because the operations do
 * not affect a running transaction).
 */
public class SynchronousTransactionAgent implements TransactionAgent {

  private static final Logger Log =
    LoggerFactory.getLogger(SynchronousTransactionAgent.class);

  // the actual operation executor
  private OperationExecutor opex;
  // the operation context for all operations
  private OperationContext context;

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public SynchronousTransactionAgent(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public void start() throws OperationException {
    // nothing to do
  }

  @Override
  public void abort() throws OperationException {
    // nothing to do
  }

  @Override
  public void finish() throws OperationException {
    // nothing to do
  }

  @Override
  public void submit(WriteOperation operation) throws OperationException {
    // execute synchronously
    this.opex.execute(this.context, operation);
  }

  @Override
  public void submit(List<WriteOperation> operations) throws OperationException {
    // execute synchronously
    this.opex.execute(this.context, operations);
  }

  @Override
  public OperationResult<Map<byte[], Long>> execute(Increment increment) throws OperationException {
    // execute synchronously - beware since this is also a write, it can't be rolled back
    return this.opex.execute(this.context, increment);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(Read read) throws OperationException {
    // execute synchronously
    return this.opex.execute(this.context, read);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(ReadColumnRange read) throws OperationException {
    // execute synchronously
    return this.opex.execute(this.context, read);
  }

  @Override
  public OperationResult<List<byte[]>> execute(ReadAllKeys read) throws OperationException {
    // execute synchronously
    return this.opex.execute(this.context, read);
  }
}
