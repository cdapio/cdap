package com.continuuity.gateway.v2.txmanager;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Transaction Manager for a single transaction.
 */
public class SingletonTxManager extends AbstractTxManager {
  private final Set<TransactionAware> txnAwares;

  public SingletonTxManager(OperationExecutor opex, TransactionAware txnAware) {
    super(opex);
    this.txnAwares = ImmutableSet.of(txnAware);
  }

  @Override
  protected Set<TransactionAware> getTransactionAwares() {
    return txnAwares;
  }
}
