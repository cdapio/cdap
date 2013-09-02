package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.transaction.Transaction;

import java.util.List;
import java.util.Map;

/**
 * This interface defines methods for submitting operations in the context of
 * a transaction. It has methods to start, commit, or abort, and to submit
 * read or write operations.
 *
 * The semantics are depending on the implementation: For instance,
 * one possible implementation is that the transaction is started immediately, and
 * all operations are executed immediately in that transaction. But a smarter
 * implementation could defer the start of the transaction to a later time, and
 * batch up all operations until then, to minimize the duration of the transaction.
 * Yet another implementation could ignore transactions alltogether and execute each
 * operation by itself.
 *
 * Transaction agents are not thread-safe - any synchronization is up to the caller.
 */
public interface TransactionAgent {

  /**
   * Start the interaction with this agent.
   * @throws OperationException if something goes wrong in data fabric
   */
  public void start() throws OperationException;

  /**
   * Start the interaction with this agent.
   * @param timeout the timeout for the transaction, or null for no timeout
   * @throws OperationException if something goes wrong in data fabric
   */
  public void start(Integer timeout) throws OperationException;

  /**
   * Ends the interaction with this agent, indicating failure. This can mean, for
   * instance, a rollback of all operations issued so far, but this is dependent on
   * the implementation.
   * @throws OperationException if something goes wrong in data fabric
   */
  public void abort() throws OperationException;

  /**
   * End the interaction with this agent, indicating success. This can mean, for
   * instance, a commit of all operations issued so far, but this is dependent on
   * the implementation.
   * @throws OperationException if something goes wrong in data fabric
   */
  public void finish() throws OperationException;

  /**
   * Flushes all buffered operations. Ensures that all buffered ops are performed.
   * This can mean e.g. that all operations become visible to other
   * transaction agents even if they use same yet to be committed transaction.
   * @throws OperationException if something goes wrong in data fabric
   */
  public void flush() throws OperationException;

  /**
   * Submit a write operation for execution.
   * @param operation The operation
   * @throws OperationException if something goes wrong in data fabric
   */
  public void submit(WriteOperation operation) throws OperationException;

  /**
   * Submit a batch of write operations for execution. Typically these operations would
   * be executed together in a transaction, but it is dependent on the implementation.
   * @param operations The operations to execute
   * @throws OperationException if something goes wrong in data fabric
   */
  public void submit(List<WriteOperation> operations) throws OperationException;

  /**
   * Execute an increment operation and return the incremented values.
   * @param increment the operation
   * @return a map from the name of each incremented column to its resulting value
   * @throws OperationException if something goes wrong in data fabric
   */
  public Map<byte[], Long> execute(Increment increment) throws OperationException;

  /**
   * Execute a read operation and return the result.
   * @param read the read operation
   * @return the result of the operation
   * @throws OperationException if something goes wrong in data fabric
   */
  public OperationResult<Map<byte[], byte[]>> execute(Read read) throws OperationException;

  /**
   * Execute a read operation over a column range and return the result.
   * @param read the read operation
   * @return the result of the operation
   * @throws OperationException if something goes wrong in data fabric
   */
  public OperationResult<Map<byte[], byte[]>> execute(ReadColumnRange read) throws OperationException;

  /**
   * Execute a read operation and return the result.
   * @param read the read operation
   * @return the result of the operation
   * @throws OperationException if something goes wrong in data fabric
   */
  public OperationResult<List<byte[]>> execute(ReadAllKeys read) throws OperationException;

  /**
   * Execute a get splits operation and return the result.
   * @param getSplits the operation
   * @return a list of key ranges, each describing a split
   * @throws OperationException if something goes wrong in data fabric
   */
  public OperationResult<List<KeyRange>> execute(GetSplits getSplits) throws OperationException;

  /**
   * Get a scanner for a table.
   * @param scan the scan to perform
   * @return a scanner
   * @throws OperationException if something goes wrong in data fabric
   */
  public Scanner scan(Scan scan) throws OperationException;

  /**
   * return the number of operations performed successfully in this transaction.
   * @return the number of operations
   */
  public int getSucceededCount();

  /**
   * return the number of operations that failed in this transaction.
   * @return the number of operations
   */
  public int getFailedCount();

  /**
   * NOTE: TransactionAgent will go away once we fully migrate to new tx system.
   * @return current value of {@link com.continuuity.data2.transaction.Transaction} or null
   */
  public Transaction getCurrentTx();
}
