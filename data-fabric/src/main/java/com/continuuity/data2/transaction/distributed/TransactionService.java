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
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class TransactionService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);

  private final ThriftRPCServer<TransactionServiceThriftHandler, TTransactionServer> server;
  private final DiscoveryService discoveryService;
  private final CConfiguration conf;
  private Cancellable cancelDiscovery;
  private ZKInterProcessReentrantLock lock;

  @Inject
  public TransactionService(@Named("TransactionServerConfig") CConfiguration conf,
                            DiscoveryService discoveryService,
                            InMemoryTransactionManager txManager) {

    this.discoveryService = discoveryService;
    this.conf = conf;

    // Retrieve the port and the number of threads for the service
    int port = conf.getInt(Constants.CFG_DATA_TX_BIND_PORT,
                            Constants.DEFAULT_DATA_TX_BIND_PORT);
    String address = conf.get(Constants.CFG_DATA_TX_BIND_ADDRESS,
                            Constants.DEFAULT_DATA_TX_BIND_ADDRESS);
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
    String quorum = conf.get(com.continuuity.common.conf.Constants.Zookeeper.QUORUM);
    ZKClient zkClient = getZkClientService(quorum);
    lock = new ZKInterProcessReentrantLock(zkClient, "/tx.service");

    Futures.addCallback(lock.acquire(), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        try {
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
        } catch (Throwable e) {
          if (cancelDiscovery != null) {
            cancelDiscovery.cancel();
            cancelDiscovery = null;
          }
          // todo: possible deadlock if de-registering fails
          if (lock != null) {
            // NOTE: should go after deregistering in discovery
            lock.release();
          }
          stopAndWait();
        }
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
    // NOTE: we release lock only after we de-register with discovery
    // todo: possible deadlock if cancelDiscovery fails :(
    if (lock != null) {
      Futures.getUnchecked(lock.release());
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

  private ZKClientService getZkClientService(String zkConnectionString) {
    ZKClientService zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zkConnectionString).setSessionTimeout(10000).build(),
          RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
        )
      )
    );
    zkClientService.startAndWait();
    return zkClientService;
  }
}
