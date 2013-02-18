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
 * This is a transaction agent that defers all writes until the finish, then
 * executes them all together as a batch transaction. Reads are performed
 * synchronously, each in its own transaction. If an error occurs during a
 * read operation, the agent remains operational (because the reads do not
 * affect the transaction).
 *
 * This class is not thread-safe - any synchronization is up to the caller.
 */
public class BatchTransactionAgentWithSyncReads implements TransactionAgent {

  private static final Logger Log =
    LoggerFactory.getLogger(BatchTransactionAgentWithSyncReads.class);

  // the actual operation executor
  private OperationExecutor opex;
  // the operation context for all operations
  private OperationContext context;
  // the write operations that were received
  private List<WriteOperation> writes = Lists.newLinkedList();

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public BatchTransactionAgentWithSyncReads(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public void start() throws OperationException {
    this.writes.clear();
  }

  @Override
  public void abort() throws OperationException {
    this.writes.clear();
  }

  @Override
  public void finish() throws OperationException {
    if (this.writes.isEmpty()) {
      return;
    }
    try {
      this.opex.commit(this.context, this.writes);
    } finally {
      this.writes.clear();
    }
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

  @Override
  public OperationResult<Map<byte[], Long>> execute(Increment increment) throws OperationException {
    // execute synchronously - beware since this is also a write, it can't be rolled back
    return this.opex.increment(this.context, increment);
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
