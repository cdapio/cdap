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
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;

import java.util.List;
import java.util.Map;

/**
 * This is a transaction agent that performs all operations immediately
 * synchronously, each in its own transaction. If an error occurs during
 * an operation, the agent remains operational (because the operations do
 * not affect a running transaction).
 *
 * Even though this class is currently thread-safe, the caller should not
 * rely on that, because transaction agents in general are not thread-safe
 * - any synchronization is up to the caller.
 */
public class SynchronousTransactionAgent extends AbstractTransactionAgent {

  /**
   * Constructor must pass the operation executor and context.
   * @param opex the actual operation executor
   * @param context the operation context for all operations
   */
  public SynchronousTransactionAgent(OperationExecutor opex,
                                     OperationContext context,
                                     Iterable<TransactionAware> txAware,
                                     TransactionSystemClient txSystemClient) {
    super(opex, context, txAware, txSystemClient);
  }

  @Override
  public void submit(WriteOperation operation) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      this.opex.commit(this.context, operation);
      success = true;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public void submit(List<WriteOperation> operations) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      this.opex.commit(this.context, operations);
      success = true;
    } finally {
      if (success) {
        succeededSome(operations.size());
      } else {
        failedSome(operations.size());
      }
    }
  }

  @Override
  public Map<byte[], Long> execute(Increment increment) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously - beware since this is also a write, it can't be rolled back
      Map<byte[], Long> result = this.opex.increment(this.context, increment);
      success = true;
      return result;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(Read read) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(this.context, read);
      success = true;
      return result;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(ReadColumnRange read) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      OperationResult<Map<byte[], byte[]>> result = this.opex.execute(this.context, read);
      success = true;
      return result;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public OperationResult<List<byte[]>> execute(ReadAllKeys read) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      OperationResult<List<byte[]>> result = this.opex.execute(this.context, read);
      success = true;
      return result;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public OperationResult<List<KeyRange>> execute(GetSplits getSplits) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      OperationResult<List<KeyRange>> result = this.opex.execute(this.context, getSplits);
      success = true;
      return result;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }

  @Override
  public Scanner scan(Scan scan) throws OperationException {
    boolean success = false;
    try {
      // execute synchronously
      Scanner scanner = this.opex.scan(this.context, null, scan);
      success = true;
      return scanner;
    } finally {
      if (success) {
        succeededOne();
      } else {
        failedOne();
      }
    }
  }
}
