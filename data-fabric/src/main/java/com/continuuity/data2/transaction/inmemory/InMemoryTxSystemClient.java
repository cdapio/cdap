package com.continuuity.data2.transaction.inmemory;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;

import java.util.Collection;

/**
 *
 */
public class InMemoryTxSystemClient implements TransactionSystemClient {
  @Override
  public Transaction start() {
    return InMemoryTransactionOracle.start();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.size() == 0) {
      return true;
    }

    return InMemoryTransactionOracle.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) {
    return InMemoryTransactionOracle.commit(tx);
  }

  @Override
  public boolean abort(Transaction tx) {
    return InMemoryTransactionOracle.abort(tx);
  }
}
