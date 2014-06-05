package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * Transaction Reactor Service Management in Distributed Mode.
 */
public class TransactionServiceManager extends AbstractDistributedReactorServiceManager {

  @Inject
  public TransactionServiceManager(TwillRunnerService twillRunnerService) {
    super(Constants.Service.TRANSACTION, twillRunnerService);
  }
}
