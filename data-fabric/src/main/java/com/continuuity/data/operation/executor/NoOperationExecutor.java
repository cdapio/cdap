package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an implementation of OperationExecutor that does nothing but
 * return empty results. It is useful for testing and performance benchmarks.
 */
public class NoOperationExecutor implements OperationExecutor {
  // Dataset and queue logic relies on tx id to grow monotonically even after restart. Hence we need to start with
  // value that is for sure bigger than the last one used before restart.
  // NOTE: with code below we assume we don't do more than 100K tx/sec
  private AtomicLong tx = new AtomicLong(System.currentTimeMillis() * 100);

  @Override
  public com.continuuity.data2.transaction.Transaction startShort() throws OperationException {
    long wp = tx.incrementAndGet();
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new com.continuuity.data2.transaction.Transaction(
      Long.MAX_VALUE - 1, wp, new long[0], new long[0],
      com.continuuity.data2.transaction.Transaction.NO_TX_IN_PROGRESS);
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws OperationException {
    return startShort();
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startLong() throws OperationException {
    return startShort();
  }

  @Override
  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException {
    return true;
  }

  @Override
  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    return true;
  }

  @Override
  public void abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    // do nothing
  }

  @Override
  public void invalidate(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    // do nothing
  }
}
