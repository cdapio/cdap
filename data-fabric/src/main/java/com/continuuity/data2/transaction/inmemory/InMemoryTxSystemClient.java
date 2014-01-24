package com.continuuity.data2.transaction.inmemory;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionNotInProgressException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.inject.Inject;

import java.util.Collection;

/**
 *
 */
public class InMemoryTxSystemClient implements TransactionSystemClient {

  InMemoryTransactionManager txManager;

  @Inject
  public InMemoryTxSystemClient(InMemoryTransactionManager txmgr) {
    txManager = txmgr;
  }

  @Override
  public Transaction startLong() {
    return txManager.startLong();
  }

  @Override
  public Transaction startShort() {
    return txManager.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    return txManager.startShort(timeout);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return changeIds.isEmpty() || txManager.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return txManager.commit(tx);
  }

  @Override
  public void abort(Transaction tx) {
    txManager.abort(tx);
  }

  @Override
  public void invalidate(Transaction tx) {
    txManager.invalidate(tx);
  }
}
