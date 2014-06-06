package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Transaction Reactor Service Management in Distributed Mode.
 */
public class TransactionServiceManager extends AbstractDistributedReactorServiceManager {
  private TransactionSystemClient txClient;

  @Inject
  public TransactionServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                   TransactionSystemClient txClient) {
    super(cConf, Constants.Service.TRANSACTION, twillRunnerService);
    this.txClient = txClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Transaction.Container.MAX_INSTANCES);
  }

  @Override
  public boolean isServiceAvailable() {
    return txClient.status().equals(Constants.Monitor.STATUS_OK);
  }
}
