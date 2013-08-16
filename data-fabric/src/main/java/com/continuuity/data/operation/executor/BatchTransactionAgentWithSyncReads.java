package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * This is a transaction agent that defers all writes until the finish, then
 * executes them all together as a batch transaction. Reads are performed
 * synchronously, each in its own transaction. If an error occurs during a
 * read operation, the agent remains operational (because the reads do not
 * affect the transaction).
 *
 * This class is not thread-safe - any synchronization is up to the caller.
 */
public class BatchTransactionAgentWithSyncReads extends SynchronousTransactionAgent {

  // the write operations that were received
  private List<WriteOperation> writes = Lists.newLinkedList();

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public BatchTransactionAgentWithSyncReads(OperationExecutor opex, OperationContext context,
                                            Iterable<TransactionAware> txAware,
                                            TransactionSystemClient txSystemClient) {
    super(opex, context, txAware, txSystemClient);
  }

  @Override
  public void start() throws OperationException {
    this.writes.clear();
    super.start();
  }

  @Override
  public void abort() throws OperationException {
    this.writes.clear();
    super.abort();
  }

  /**
   * Equivalent to {@link #finish()} for this agent type.
   * @throws OperationException
   */
  @Override
  public void flush() throws OperationException {
    finish();
  }

  @Override
  public void finish() throws OperationException {
    if (this.writes.isEmpty()) {
      return;
    }
    // execute the accumulated batch in a new transaction
    super.submit(this.writes);
  }

  @Override
  public void submit(WriteOperation operation) throws OperationException {
    // don't execute, but just add to the batch
    this.writes.add(operation);
  }

  @Override
  public void submit(List<WriteOperation> operations) throws OperationException {
    // don't execute, but just add to the batch
    this.writes.addAll(operations);
  }
}
