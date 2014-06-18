package com.continuuity;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionService;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TransactionWrapperService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionWrapperService.class);

  private final InMemoryTransactionService transactionService;

  @Inject
  public TransactionWrapperService(InMemoryTransactionService txService) {
    this.transactionService = txService;
  }

  @Override
  protected void doStart() {
    transactionService.startAndWait();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.TRANSACTION));
    LOG.info("Started Transaction Service...");
    notifyStarted();
  }

  @Override
  protected void doStop() {
    transactionService.stopAndWait();
    notifyStopped();
  }
}
