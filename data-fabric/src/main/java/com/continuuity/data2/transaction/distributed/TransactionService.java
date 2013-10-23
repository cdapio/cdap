/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.rpc.ThriftRPCServer;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Transaction server that manages transaction data for the Reactor.
 * <p>
 *   Transaction server is HA, one can start multiple instances, only one of which is active and will register itself in
 *   discovery service.
 * </p>
 */
public final class TransactionService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);

  private final DiscoveryService discoveryService;
  private final Provider<InMemoryTransactionManager> txManagerProvider;
  private final CConfiguration conf;
  private LeaderElection leaderElection;
  private Cancellable cancelDiscovery;
  private ZKClientService zkClient;

  // thrift server config
  private final int port;
  private final String address;
  private final int threads;
  private final int ioThreads;
  private final int maxReadBufferBytes;

  private ThriftRPCServer<TransactionServiceThriftHandler, TTransactionServer> server;

  @Inject
  public TransactionService(@Named("TransactionServerConfig") CConfiguration conf,
                            DiscoveryService discoveryService,
                            Provider<InMemoryTransactionManager> txManagerProvider) {

    this.discoveryService = discoveryService;
    this.txManagerProvider = txManagerProvider;
    this.conf = conf;

    // Retrieve the port and the number of threads for the service
    port = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_BIND_PORT,
                       Constants.Transaction.Service.DEFAULT_DATA_TX_BIND_PORT);
    address = conf.get(Constants.Transaction.Service.CFG_DATA_TX_BIND_ADDRESS,
                       Constants.Transaction.Service.DEFAULT_DATA_TX_BIND_ADDRESS);
    threads = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_SERVER_THREADS,
                          Constants.Transaction.Service.DEFAULT_DATA_TX_SERVER_THREADS);
    ioThreads = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_SERVER_IO_THREADS,
                            Constants.Transaction.Service.DEFAULT_DATA_TX_SERVER_IO_THREADS);


    maxReadBufferBytes = conf.getInt(com.continuuity.common.conf.Constants.Thrift.MAX_READ_BUFFER,
                                     com.continuuity.common.conf.Constants.Thrift.DEFAULT_MAX_READ_BUFFER);

    LOG.info("Configuring TransactionService" +
               ", address: " + address +
               ", port: " + port +
               ", threads: " + threads +
               ", io threads: " + ioThreads +
               ", max read buffer (bytes): " + maxReadBufferBytes);
  }

  @Override
  protected void doStart() {
    String quorum = conf.get(com.continuuity.common.conf.Constants.Zookeeper.QUORUM);
    zkClient = getZkClientService(quorum);
    leaderElection = new LeaderElection(zkClient, "/tx.service/leader", new ElectionHandler() {
      @Override
      public void leader() {
        // if the txManager fails, we should stop the server
        InMemoryTransactionManager txManager = txManagerProvider.get();
        txManager.addListener(new Listener() {
          @Override
          public void starting() {
          }

          @Override
          public void running() {
          }

          @Override
          public void stopping(State from) {
          }

          @Override
          public void terminated(State from) {
          }

          @Override
          public void failed(State from, Throwable failure) {
            LOG.error("Transaction manager aborted, stopping transaction service");
            stopAndWait();
          }
        }, MoreExecutors.sameThreadExecutor());

        server = ThriftRPCServer.builder(TTransactionServer.class)
          .setHost(address)
          .setPort(port)
          .setWorkerThreads(threads)
          .setMaxReadBufferBytes(maxReadBufferBytes)
          .setIOThreads(ioThreads)
          .build(new TransactionServiceThriftHandler(txManager));
        try {
          server.startAndWait();
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
        } catch (Throwable t) {
          leaderElection.cancel();
          notifyFailed(t);
        }
      }

      @Override
      public void follower() {
        // NOTE: it is guaranteed that we are notified before others if we gave up leadership
        if (cancelDiscovery != null) {
          cancelDiscovery.cancel();
        }
        if (server != null && server.isRunning()) {
          server.stopAndWait();
        }
      }
    });

    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (leaderElection != null) {
      // NOTE: if was a leader this will cause loosing of leadership which in callback above will
      //       de-register service in discovery service and stop the service if needed
      try {
        leaderElection.cancelAndWait(5000L);
      } catch (TimeoutException te) {
        LOG.warn("Timed out waiting for leader election cancellation to complete");
      }
    }
    if (zkClient != null && zkClient.isRunning()) {
      zkClient.stop();
    }

    notifyStopped();
  }

  private ZKClientService getZkClientService(String zkConnectionString) {
    ZKClientService zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zkConnectionString)
            .setSessionTimeout(conf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                            Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
            .build(),
          RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
        )
      )
    );
    zkClientService.startAndWait();
    return zkClientService;
  }
}
