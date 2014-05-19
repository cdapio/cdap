package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.TransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;

import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the tx system client that doesn't talk to any global service and tries to do its best to meet the
 * tx system requirements/expectations. In fact it implements enough logic to support running flows (when each flowlet
 * uses its own detached tx system client, without talking to each other and sharing any state) with "process exactly
 * once" guarantee if no failures happen.
 *
 * NOTE: Will NOT detect conflicts. May leave inconsistent state when process crashes. Does NOT provide even read
 *       isolation guarantees.
 *
 * Good for performance testing. For demoing high throughput. For use-cases with relaxed tx guarantees.
 */
public class DetachedTxSystemClient implements TransactionSystemClient {
  // Dataset and queue logic relies on tx id to grow monotonically even after restart. Hence we need to start with
  // value that is for sure bigger than the last one used before restart.
  // NOTE: with code below we assume we don't do more than InMemoryTransactionManager.MAX_TX_PER_MS tx/ms
  //       by single client
  private AtomicLong generator = new AtomicLong(System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS);

  @Override
  public com.continuuity.data2.transaction.Transaction startShort() {
    long wp = generator.incrementAndGet();
    // NOTE: using InMemoryTransactionManager.MAX_TX_PER_MS to be at least close to real one
    long now = System.currentTimeMillis();
    if (wp < now * TxConstants.MAX_TX_PER_MS) {
      // trying to advance to align with timestamp, but only once: if failed, we'll just try again later with next tx
      long advanced = now * TxConstants.MAX_TX_PER_MS;
      if (generator.compareAndSet(wp, advanced)) {
        wp = advanced;
      }
    }
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new com.continuuity.data2.transaction.Transaction(
      Long.MAX_VALUE - 1, wp, new long[0], new long[0],
      com.continuuity.data2.transaction.Transaction.NO_TX_IN_PROGRESS);
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startShort(int timeout) {
    return startShort();
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startLong() {
    return startShort();
  }

  @Override
  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds) {
    return true;
  }

  @Override
  public boolean commit(com.continuuity.data2.transaction.Transaction tx) {
    return true;
  }

  @Override
  public void abort(com.continuuity.data2.transaction.Transaction tx) {
    // do nothing
  }

  @Override
  public boolean invalidate(long tx) {
    return true;
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    throw new TransactionCouldNotTakeSnapshotException(
        "Snapshot not implemented in detached transaction system client");
  }

  @Override
  public String status() {
    return Constants.Monitor.STATUS_OK;
  }

  @Override
  public void resetState() {
    // do nothing
  }
}
