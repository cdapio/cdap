/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.TxConstants;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Transaction server that manages transaction data for the Reactor.
 * <p>
 *   Transaction server is HA, one can start multiple instances, only one of which is active and will register itself in
 *   discovery service.
 * </p>
 */
public class InMemoryTransactionService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTransactionService.class);

  private final DiscoveryService discoveryService;
  protected final Provider<InMemoryTransactionManager> txManagerProvider;
  private Cancellable cancelDiscovery;
  protected InMemoryTransactionManager txManager;

  // thrift server config
  protected final String address;
  protected final int threads;
  protected final int ioThreads;
  protected final int maxReadBufferBytes;

  @Inject
  public InMemoryTransactionService(CConfiguration conf,
                            DiscoveryService discoveryService,
                            Provider<InMemoryTransactionManager> txManagerProvider) {

    this.discoveryService = discoveryService;
    this.txManagerProvider = txManagerProvider;

    address = conf.get(Constants.Transaction.Container.ADDRESS);

    // Retrieve the number of threads for the service
    threads = conf.getInt(TxConstants.Service.CFG_DATA_TX_SERVER_THREADS,
                          TxConstants.Service.DEFAULT_DATA_TX_SERVER_THREADS);
    ioThreads = conf.getInt(TxConstants.Service.CFG_DATA_TX_SERVER_IO_THREADS,
                            TxConstants.Service.DEFAULT_DATA_TX_SERVER_IO_THREADS);

    maxReadBufferBytes = conf.getInt(com.continuuity.common.conf.Constants.Thrift.MAX_READ_BUFFER,
                                     com.continuuity.common.conf.Constants.Thrift.DEFAULT_MAX_READ_BUFFER);

    LOG.info("Configuring TransactionService" +
               ", address: " + address +
               ", threads: " + threads +
               ", io threads: " + ioThreads +
               ", max read buffer (bytes): " + maxReadBufferBytes);
  }

  protected void undoRegiser() {
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }
  }

  protected void doRegister() {
    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return com.continuuity.common.conf.Constants.Service.TRANSACTION;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return getAddress();
      }
    });
  }

  protected InetSocketAddress getAddress() {
    return new InetSocketAddress(1);
  }

  @Override
  protected void doStart() {
    try {
      txManager = txManagerProvider.get();
      txManager.startAndWait();
      doRegister();
      LOG.info("Transaction Thrift service started successfully on " + getAddress());
      notifyStarted();
    } catch (Throwable t) {
      LOG.info("Transaction Thrift service didn't start on " + getAddress());
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    undoRegiser();
    txManager.stopAndWait();
    notifyStopped();
  }

}
