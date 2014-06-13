package com.continuuity.internal.app.runtime.batch;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.InMemoryReactorServiceManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.inject.Inject;

/**
 *
 */
public class InMemoryTransactionServiceManager extends InMemoryReactorServiceManager {
  private TransactionSystemClient txClient;

  @Override
  public boolean isLogAvailable() {
    return false;
  }

  @Inject
  public InMemoryTransactionServiceManager(TransactionSystemClient txClient) {
    this.txClient = txClient;
  }

  @Override
  public boolean isServiceAvailable() {
    return txClient.status().equals(Constants.Monitor.STATUS_OK);
  }

}
