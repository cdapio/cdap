package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This transaction agent defers all operations as long as possible while
 * preserving transactional semantics:
 * <ul>
 *   <li>Write operations are deferred until a read operation is executed, or a limit
 *   on the number or aggregate size or deferred operations is reached.</li>
 *   <li>When a read operation is received, it must return a value and thus
 *     execute immediately. Because it may depend on the writes that were already
 *     submitted, all deferred write operations are executed before the read.</li>
 *   <li>An increment counts as a read because it returns a result.</li>
 *   <li>Whenever the number or aggregate size of deferred operations crosses a limit,
 *     all deferred operations are executed.</li>
 *   <li>The start of the transaction is deferred until the first operation is
 *     actually executed.</li>
 *   <li>Upon finish, all outstanding operations are executed, and the transaction
 *     is committed.</li>
 * </ul>
 * This agent aborts the current transaction whenever an operation fails - even if
 * it is a read operation. After a failure, no more operations may be submitted.
 *
 * This class is not thread-safe - any synchronization is up to the caller.
 */
public class SmartTransactionAgent extends AbstractTransactionAgent {

  private static final Logger Log =
    LoggerFactory.getLogger(SmartTransactionAgent.class);

  // the list of currently deferred operations
  private final List<WriteOperation> deferred = Lists.newLinkedList();
  // aggregate size of current deferred operations
  private int deferredSize;
  // the current transaction
  private Transaction xaction;
  // keep track of current state
  protected State state = State.New;

  // keep track of successful operations: we can't increase succeeded count every time we
  // execute a deferred batch, we will only know whether they succeed at commit().
  private AtomicInteger executed = new AtomicInteger(0);

  /**
   * @return how many operations have been executed (excluding the current deferred operations)
   */
  public int getExecutedCount() {
    return executed.get();
  }

  /**
   * helper enum.
   */
  protected enum State { New, Running, Aborted, Finished }

  // defaults for limits on deferred operations
  public static final int DEFAULT_SIZE_LIMIT = 16 * 1024 * 1024;
  public static final int DEFAULT_COUNT_LIMIT = 1000;

  // limit for total size of deferred operations
  private int sizeLimit = DEFAULT_SIZE_LIMIT;
  // limit for number of deferred operations
  private int countLimit = DEFAULT_COUNT_LIMIT;

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public SmartTransactionAgent(OperationExecutor opex, OperationContext context,
                               Iterable<TransactionAware> txAware, TransactionSystemClient txSystemClient) {
    this(opex, context, txAware, txSystemClient, null);
  }

  /**
   * Same as {@link #SmartTransactionAgent(OperationExecutor, com.continuuity.data.operation.OperationContext,
   * java.lang.Iterable, com.continuuity.data2.transaction.TransactionSystemClient)} but
   * takes transaction to operate with.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   * @param tx optional transaction to use for all operations, if not provided this agent will use new one
   */
  public SmartTransactionAgent(OperationExecutor opex,
                               OperationContext context,
                               Iterable<TransactionAware> txAware,
                               TransactionSystemClient txSystemClient,
                               Transaction tx) {
    super(opex, context, txAware, txSystemClient);
    this.xaction = tx;
  }

  /**
   * Set the limit for the aggregate size of deferred operations.
   * May not be called after the transaction has started
   * @param sizeLimit the new limit
   */
  public void setSizeLimit(int sizeLimit) {
    if (this.state != State.New) {
      // configuration change is only allowed before the ransaction starts
      throw new IllegalStateException("State must be New to change limits.");
    }
    this.sizeLimit = sizeLimit;
  }

  /**
   * Set the limit for the number of deferred operations.
   * May not be called after the transaction has started.
   * @param countLimit the new limit
   */
  public void setCountLimit(int countLimit) {
    if (this.state != State.New) {
      // configuration change is only allowed before the ransaction starts
      throw new IllegalStateException("State must be New to change limits.");
    }
    this.countLimit = countLimit;
  }

  // helper to clear the list of deferred operations
  private void clearDeferred() {
    this.deferred.clear();
    this.deferredSize = 0;
  }

  @Override
  public void start() throws OperationException {
    // Transaction agent can be started only once
    if (this.state != State.New) {
      // in this case we want to throw a runtime exception. The transaction has started
      // and we must abort or commit it, otherwise data fabric may be inconsistent.
      throw new IllegalStateException("Transaction has already started.");
    }
    super.start();
    this.state = State.Running;
  }

  @Override
  public void start(Integer timeout) throws OperationException {
    // Transaction agent can be started only once
    if (this.state != State.New) {
      // in this case we want to throw a runtime exception. The transaction has started
      // and we must abort or commit it, otherwise data fabric may be inconsistent.
      throw new IllegalStateException("Transaction has already started.");
    }
    super.start(timeout);
    this.state = State.Running;
  }

  @Override
  public void abort() throws OperationException {
    super.abort();

    if (this.state == State.Aborted || this.state == State.Finished) {
      // might be called by some generic exception handler even though already aborted/finished - we allow that
      return;
    }

    // everything executed so far is now considered as failed.
    // also, set the executed count back to 0 to avoid double counting in case abort() is called again
    failedSome(this.executed.getAndSet(0));

    // drop the deferred ops
    if (!this.deferred.isEmpty()) {
      // deferred operations now count as failed
      failedSome(this.deferred.size());
      clearDeferred();
    }

    try {
      abortTransaction();
    } finally {
      this.state = State.Aborted;
    }
  }

  /**
   * Called to abort transaction. Subclasses can override it, e.g. to skip transaction abort.
   * @throws OperationException
   */
  protected void abortTransaction() throws OperationException {
    try {
      if (this.xaction != null) {
        // transaction was started - it must be aborted
        this.opex.abort(this.context, this.xaction);
      }
    } catch (OperationException e) {
      // gracefully deal with abort of transaction that is already aborted
      if (e.getStatus() != StatusCode.INVALID_TRANSACTION) {
        throw e;
      }
    } finally {
      this.xaction = null;
    }
  }

  @Override
  public void finish() throws OperationException {
    if (this.state != State.Running) {
      // we don't throw an exception here, but log a warning.
      // throw new IllegalStateException("Cannot finish because not running.");
      Log.warn("Attempt to finish a smart transaction that is " + this.state.name());
      return;
    }
    // add the current deferred batch to the number of executed ops
    // it will be counted either as succeeded or failed a few lines down
    this.executed.addAndGet(this.deferred.size());
    try {
      // Until we migrate completely on new tx system, two tx systems are separate. We may be in inconsistent state
      // when we committed work for one of them and not for other. It is more likely that user code (dataset ops, etc)
      // will fail during commit, so we
      super.finish();
      if (this.xaction == null) {
        if (this.deferred.isEmpty()) {
          return;
        }
        // we have no transaction yet, but we have operations:
        // execute them (together in a transaction internal to opex).
        this.opex.commit(this.context, deferred);
      } else if (this.deferred.isEmpty()) {
        // we have a transaction but no deferred ops: commit
        this.opex.commit(this.context, this.xaction);
      } else {
        // we have deferred operations and a transaction
        this.opex.commit(this.context, this.xaction, this.deferred);
      }
      // everything executed so far is now considered as succeeded.
      // also, set the executed count back to 0 to avoid double counting in case abort is called later-on
      succeededSome(this.executed.getAndSet(0));
      this.state = State.Finished;

    } catch (OperationException e) {
      // everything executed so far is now considered as failed.
      // also, set the executed count back to 0 to avoid double counting in case abort is called later-on
      failedSome(this.executed.getAndSet(0));
      this.state = State.Aborted;
      throw e;
    } finally {
      this.xaction = null;
      clearDeferred();
    }
  }

  @Override
  public void flush() throws OperationException {
    super.flush();
    // check state and get rid of deferred operations
    executeDeferred();
  }

  @Override
  public void submit(WriteOperation operation) throws OperationException {
    if (this.state != State.Running) {
      // in this case we want to throw a runtime exception. If we silently accept
      // operation(s), then these will most likely never be executed.
      throw new IllegalStateException("State must be Running to submit operations.");
    }
    // defer the operation
    this.deferred.add(operation);
    this.deferredSize += operation.getSize();
    checkLimits();
  }

  @Override
  public void submit(List<WriteOperation> operations) throws OperationException {
    if (this.state != State.Running) {
      // in this case we want to throw a runtime exception. If we silently accept
      // operation(s), then these will most likely never be executed.
      throw new IllegalStateException("State must be Running to submit operations.");
    }
    // defer these operations
    this.deferred.addAll(operations);
    for (WriteOperation operation : operations) {
      this.deferredSize += operation.getSize();
    }
    checkLimits();
  }

  // helper to check whether the deferred operations exceed the limit and, if so, execute them
  private void checkLimits() throws OperationException {
    if (this.deferred.size() > this.countLimit || this.deferredSize > this.sizeLimit) {
      executeDeferred();
    }
  }

  // helper to execute all deferred writes before a read and ensure we have a transaction
  // also, if anything fails, make sure to clear the xaction
  private void executeDeferred() throws OperationException {
    if (this.state != State.Running) {
      // in this case we want to throw a runtime exception. If we silently accept
      // operation(s), then their behavior will be unpredictable;
      throw new IllegalStateException("State must be Running to submit operations.");
    }
    if (!this.deferred.isEmpty()) {
      try {
        // these operations will now count as executed (but not as successful yet)
        this.executed.addAndGet(this.deferred.size());
        // this will start, use and return a new transaction if xaction is null
        this.xaction = this.opex.execute(this.context, this.xaction, this.deferred);
        clearDeferred();
      } catch (OperationException e) {
        // everything executed so far is now considered as failed.
        // also, set the executed count back to 0 to avoid double counting in case abort is called later-on
        failedSome(this.executed.getAndSet(0));
        // opex aborts the transaction if the execute fails
        this.xaction = null;
        clearDeferred();
        this.state = State.Aborted;
        throw e;
      }
    } else if (this.xaction == null) {
      // starting transaction that tracks changes by default
      this.xaction = opex.startTransaction(this.context, true);
    }
  }

  @Override
  public Map<byte[], Long> execute(Increment increment) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.increment(this.context, this.xaction, increment));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(Read read) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.execute(this.context, this.xaction, read));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(ReadColumnRange read) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.execute(this.context, this.xaction, read));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  @Override
  public OperationResult<List<byte[]>> execute(ReadAllKeys read) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.execute(this.context, this.xaction, read));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  @Override
  public OperationResult<List<KeyRange>> execute(GetSplits getSplits) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.execute(this.context, this.xaction, getSplits));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  @Override
  public Scanner scan(Scan scan) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return succeededOne(this.opex.scan(this.context, this.xaction, scan));
    } catch (OperationException e) {
      this.failedOne();
      this.abort();
      throw e;
    }
  }

  // commodity method
  private <T> T succeededOne(T result) {
    succeededOne();
    return result;
  }
}
