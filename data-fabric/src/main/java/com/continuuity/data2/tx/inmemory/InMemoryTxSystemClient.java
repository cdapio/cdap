package com.continuuity.data2.tx.inmemory;

import com.continuuity.data2.tx.Tx;
import com.continuuity.data2.tx.TxSystemClient;

import java.util.Collection;

/**
 *
 */
public class InMemoryTxSystemClient implements TxSystemClient {
  @Override
  public Tx start() {
    return InMemoryOracle.start();
  }

  @Override
  public boolean hasConflicts(Tx tx, Collection<byte[]> changeIds) {
    return InMemoryOracle.canCommit(tx, changeIds);
  }

  @Override
  public boolean makeVisible(Tx tx) {
    return InMemoryOracle.commit(tx);
  }
}
