/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.rpc.ThriftRPCServer;
import com.continuuity.data.operation.executor.remote.Constants;
import com.continuuity.data.transaction.thrift.TTransactionService;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;

import java.net.InetSocketAddress;

/**
 *
 */
public final class TransactionServer extends AbstractService {

  private final ThriftRPCServer<DistributedTransactionServiceHandler, TTransactionService> server;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public TransactionServer(CConfiguration cConf,
                           DiscoveryService discoveryService,
                           TransactionSystemClient transactionSystem) {
    // Retrieve the port and the number of threads for the service
    // TODO: Temporarily using random port, until the old opex is displaced by this new one.
    int port = 0;
    String address = cConf.get(Constants.CFG_DATA_OPEX_SERVER_ADDRESS,
                               Constants.DEFAULT_DATA_OPEX_SERVER_ADDRESS);
    int threads = cConf.getInt(Constants.CFG_DATA_OPEX_SERVER_THREADS,
                               Constants.DEFAULT_DATA_OPEX_SERVER_THREADS);

    this.server = ThriftRPCServer.builder(TTransactionService.class)
                                 .setHost(address)
                                 .setPort(port)
                                 .setWorkerThreads(threads)
                                 .build(new DistributedTransactionServiceHandler(transactionSystem));

    this.discoveryService = discoveryService;
  }

  @Override
  protected void doStart() {
    Futures.addCallback(server.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        cancelDiscovery = discoveryService.register(new Discoverable() {
          @Override
          public String getName() {
            return com.continuuity.common.conf.Constants.SERVICE_TRANSACTION_SERVER;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return server.getBindAddress();
          }
        });
        notifyStarted();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }
    Futures.addCallback(server.stop(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        notifyStopped();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }
}
