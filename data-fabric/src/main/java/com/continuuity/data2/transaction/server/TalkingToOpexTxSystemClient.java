package com.continuuity.data2.transaction.server;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.util.Collection;

/**
 * Temporary tx client that talks to tx server thru opex (easier faster integration).
 */
// todo: handle exceptions well
public class TalkingToOpexTxSystemClient implements TransactionSystemClient {
  private final OperationExecutor opex;

  @Inject
  public TalkingToOpexTxSystemClient(OperationExecutor opex) {
    this.opex = opex;
  }

  @Override
  public Transaction startShort() {
    try {
      return opex.startShort();
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction startShort(int timeout) {
    try {
      return opex.startShort(timeout);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction startLong() {
    try {
      return opex.startLong();
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    if (changeIds.isEmpty()) {
      return true;
    }

    try {
      return opex.canCommit(tx, changeIds);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean commit(Transaction tx) {
    try {
      return opex.commit(tx);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void abort(Transaction tx) {
    try {
      opex.abort(tx);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void invalidate(Transaction tx) {
    try {
      opex.invalidate(tx);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

}
