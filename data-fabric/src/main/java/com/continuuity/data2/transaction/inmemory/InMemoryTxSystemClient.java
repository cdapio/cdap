package com.continuuity.data2.transaction.inmemory;

import com.continuuity.data2.transaction.Transaction;
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
  public Transaction start() {
    return txManager.start();
  }

  @Override
  public Transaction start(Integer timeout) {
    return txManager.start(timeout);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.size() == 0) {
      return true;
    }
    return txManager.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) {
    return txManager.commit(tx);
  }

  @Override
  public boolean abort(Transaction tx) {
    return txManager.abort(tx);
  }
}
