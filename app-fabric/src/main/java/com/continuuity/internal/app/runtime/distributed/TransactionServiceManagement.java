package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManagement;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 *
 */
public class TransactionServiceManagement extends AbstractDistributedReactorServiceManagement {

  @Inject
  public TransactionServiceManagement(TwillRunnerService twillRunnerService) {
    super(Constants.Service.TRANSACTION, twillRunnerService);
  }
}
