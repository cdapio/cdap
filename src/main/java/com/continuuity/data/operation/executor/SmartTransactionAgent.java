package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.WriteOperation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This transaction agent defers all operations as long as possible while
 * preserving transactional semantics:
 * <ul>
 *   <li>write operations are deferred until a read operation is executed.</li>
 *   <li>when a read operation is received, it must return a value and thus
 *     execute immediately. Because it may depend on the writes that were already
 *     submitted, all deferred write operations are executed before the read.</li>
 *   <li>An increment counts as a read because it returns a result.</li>
 *   <li>The start of the transaction is deferred until the first operation is
 *     actually executed.</li>
 *   <li>Upon finish, all outstanding operations are executed, and the transaction
 *     is committed.</li>
 * </ul>
 * This agent aborts the current transaction whenever an operation fails - even if
 * it is a read operation. After a failure, no more operations may be submitted.
 *
 * This class is not thread-safe - any synchronization is up to the caller.
<<<<<<< HEAD
=======
 *
>>>>>>> master
 */
public class SmartTransactionAgent implements TransactionAgent {

  private static final Logger Log =
    LoggerFactory.getLogger(SmartTransactionAgent.class);

  // the actual operation executor
  private final OperationExecutor opex;
  // the operation context for all operations
  private final OperationContext context;
  // the list of currently deferred operations
  private final List<WriteOperation> deferred = Lists.newLinkedList();
  // the current transaction
  private Transaction xaction;
  // keep track of current state
  private State state = State.New;

  // helper enum
  private enum State { New, Running, Aborted, Finished }

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public SmartTransactionAgent(OperationExecutor opex, OperationContext context) {
    this.opex = opex;
    this.context = context;
  }

  @Override
  public void start() throws OperationException {
    if (this.state == State.Running) {
      // in this case we want to throw a runtime exception. The transaction has started
      // and we must abort or commit it, otherwise data fabric may be inconsistent.
      throw new IllegalStateException("Transaction has already started.");
    }
    this.xaction = null;
    this.deferred.clear();
    this.state = State.Running;
  }

  @Override
  public void abort() throws OperationException {
    if (this.state == State.Aborted || this.state == State.Finished) {
      // might be called by some generic exception handler even though already aborted/finished - we allow that
      return;
    }

    // drop the deferred ops
    this.deferred.clear();

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
      this.state = State.Aborted;
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
    try {
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
    } catch (OperationException e) {
      this.state = State.Aborted;
      throw e;
    } finally {
      this.xaction = null;
      this.deferred.clear();
    }
    this.state = State.Finished;
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
        // this will start, use and return a new transaction if xaction is null
        this.xaction = this.opex.execute(this.context, this.xaction, this.deferred);
        this.deferred.clear();
      } catch (OperationException e) {
        // opex aborts the transaction if the execute fails
        this.xaction = null;
        this.deferred.clear();
        this.state = State.Aborted;
        throw e;
      }
    }
    else if (this.xaction == null) {
      this.xaction = opex.startTransaction(this.context);
    }
  }

  @Override
  public Map<byte[], Long> execute(Increment increment) throws OperationException {
    // check state and get rid of deferred operations
    executeDeferred();
    // now execute the operation and make sure abort in case of failure
    try {
      return this.opex.increment(this.context, this.xaction, increment);
    } catch (OperationException e) {
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
      return this.opex.execute(this.context, this.xaction, read);
    } catch (OperationException e) {
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
      return this.opex.execute(this.context, this.xaction, read);
    } catch (OperationException e) {
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
      return this.opex.execute(this.context, this.xaction, read);
    } catch (OperationException e) {
      this.abort();
      throw e;
    }
  }
}
