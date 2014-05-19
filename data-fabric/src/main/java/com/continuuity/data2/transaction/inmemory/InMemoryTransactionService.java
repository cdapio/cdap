package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.Constants;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Starts TxManager and also registers in discoveryService
 */
public class InMemoryTransactionService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransactionService.class);

  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;
  private final Provider<InMemoryTransactionManager> txManagerProvider;
  private InMemoryTransactionManager txManager;

  @Inject
  public InMemoryTransactionService(DiscoveryService discoveryService,
                                    Provider<InMemoryTransactionManager> txManagerProvider) {
    this.discoveryService = discoveryService;
    this.txManagerProvider = txManagerProvider;
    txManager = txManagerProvider.get();

    LOG.info("Configuring TransactionService");
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Transaction Service...");
    txManager.startAndWait();
    LOG.info("Started Transaction Service...");
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.TRANSACTION;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(1);
      }
    });

    LOG.info("Transaction Service started successfully");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Transaction Service...");
    cancelDiscovery.cancel();
    txManager.stopAndWait();
  }
}
