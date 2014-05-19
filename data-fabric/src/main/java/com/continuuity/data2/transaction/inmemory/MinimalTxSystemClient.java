package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.TransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.TransactionSystemClient;

import java.io.InputStream;
import java.util.Collection;

/**
 * Dummy implementation of TxSystemClient. May be useful for perf testing.
 */
public class MinimalTxSystemClient implements TransactionSystemClient {
  private long currentTxPointer = 1;

  @Override
  public com.continuuity.data2.transaction.Transaction startShort() {
    long wp = currentTxPointer++;
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
    throw new TransactionCouldNotTakeSnapshotException("Not snapshot to take.");
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
