/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.rpc.ThriftRPCServer;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public final class TransactionService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);

  private final ThriftRPCServer<TransactionServiceThriftHandler, TTransactionServer> server;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  public TransactionService(CConfiguration conf,
                            DiscoveryService discoveryService,
                            InMemoryTransactionManager txManager) {

    this.discoveryService = discoveryService;

    // Retrieve the port and the number of threads for the service
    int port = conf.getInt(Constants.CFG_DATA_TX_SERVER_PORT,
                            Constants.DEFAULT_DATA_TX_SERVER_PORT);
    String address = conf.get(Constants.CFG_DATA_TX_SERVER_ADDRESS,
                            Constants.DEFAULT_DATA_TX_SERVER_ADDRESS);
    int threads = conf.getInt(Constants.CFG_DATA_TX_SERVER_THREADS,
                               Constants.DEFAULT_DATA_TX_SERVER_THREADS);
    int ioThreads = conf.getInt(Constants.CFG_DATA_TX_SERVER_IO_THREADS,
                                 Constants.DEFAULT_DATA_TX_SERVER_IO_THREADS);

    Log.info("Configuring TxService" +
      ", address: " + address +
      ", port: " + port +
      ", threads: " + threads +
      ", io threads: " + ioThreads);

    // ENG-443 - Set the max read buffer size. This is important as this will
    // prevent the server from throwing OOME if telnetd to the port
    // it's running on.
//      serverArgs.maxReadBufferBytes =
//                conf.getInt(com.continuuity.common.conf.Constants.Thrift.MAX_READ_BUFFER,
//                            com.continuuity.common.conf.Constants.Thrift.DEFAULT_MAX_READ_BUFFER);

    server = ThriftRPCServer.builder(TTransactionServer.class)
      .setHost(address)
      .setPort(port)
      .setWorkerThreads(threads)
      .setIOThreads(ioThreads)
      .build(new TransactionServiceThriftHandler(txManager));
  }

  @Override
  protected void doStart() {
    Futures.addCallback(server.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        cancelDiscovery = discoveryService.register(new Discoverable() {
          @Override
          public String getName() {
            return com.continuuity.common.conf.Constants.Service.TRANSACTION;
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
