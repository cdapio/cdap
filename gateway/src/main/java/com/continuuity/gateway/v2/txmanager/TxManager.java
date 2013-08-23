package com.continuuity.gateway.v2.txmanager;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Set;

/**
 * Transaction manager to handle transactions.
 */
public class TxManager extends AbstractTxManager {

  private final Set<TransactionAware> txAwares =
    Collections.newSetFromMap(Maps.<TransactionAware, Boolean>newConcurrentMap());

  public TxManager(OperationExecutor opex) {
    super(opex);
  }

  /**
   * Add new transaction aware object. Addition can be done at any time, even when a txn is ongoing.
   * @param txAware transaction aware object to be added.
   */
  public void add(TransactionAware txAware) {
    txAwares.add(txAware);
  }

  /**
   * Remove a transaction aware object. Removal can be done only when txn is not ongoing.
   * @param txAware transaction aware object to be removed.
   */
  public void remove(TransactionAware txAware) {
    txAwares.remove(txAware);
  }

  @Override
  protected Set<TransactionAware> getTransactionAwares() {
    return txAwares;
  }
}
