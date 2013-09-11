package com.continuuity.data2.transaction;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;

/**
 *
 */
public class TransactionExecutor {

  final Collection<TransactionAware> txAwares;
  final TransactionSystemClient txClient;


  public TransactionExecutor(Collection<TransactionAware> txAwares, TransactionSystemClient txClient) {
    this.txAwares = ImmutableList.copyOf(txAwares);
    this.txClient = txClient;
  }


  public <I, O> O execute(Function<I, O> function, I input) {
    Transaction tx = start();
    O o;
    try {
      o = function.apply(input);
    } catch (Exception e) {
      abort(tx);
      throw Throwables.propagate(e);
    }
    Collection<byte[]> changes = Lists.newArrayList();
    txClient.canCommit(tx, changes);
    return o;
  }

  private Transaction start() {
    Transaction tx = txClient.startShort();
    for (TransactionAware txAware : txAwares) {
      txAware.startTx(tx);
    }
    return tx;
  }

  private void abort(Transaction tx) {
    boolean success = true;
    for (TransactionAware txAware : txAwares) {
      try {
        success = success && txAware.rollbackTx();
      } catch (Exception e) {
        success = false;
      }
    }
    if (success) {
      txClient.abort(tx);
    } else {
      txClient.invalidate(tx);
    }
  }
}
