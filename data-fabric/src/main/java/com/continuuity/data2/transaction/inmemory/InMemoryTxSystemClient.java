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
    return InMemoryOracle.start();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.size() == 0) {
      return false;
    }

    return InMemoryOracle.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) {
    return InMemoryOracle.commit(tx);
  }

  @Override
  public boolean abort(Transaction tx) {
    return InMemoryOracle.abort(tx);
  }
}
