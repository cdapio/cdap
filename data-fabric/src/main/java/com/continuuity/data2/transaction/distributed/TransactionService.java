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
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
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
  private final ZKClient zkClient;
  private LeaderElection leaderElection;
  private Cancellable cancelDiscovery;

  // thrift server config
  private final String address;
  private final int threads;
  private final int ioThreads;
  private final int maxReadBufferBytes;

  private ThriftRPCServer<TransactionServiceThriftHandler, TTransactionServer> server;

  @Inject
  public TransactionService(@Named("TransactionServerConfig") CConfiguration conf,
                            ZKClient zkClient,
                            DiscoveryService discoveryService,
                            Provider<InMemoryTransactionManager> txManagerProvider) {

    this.discoveryService = discoveryService;
    this.txManagerProvider = txManagerProvider;
    this.zkClient = zkClient;

    address = conf.get(Constants.Transaction.Container.ADDRESS);

    // Retrieve the number of threads for the service
    threads = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_SERVER_THREADS,
                          Constants.Transaction.Service.DEFAULT_DATA_TX_SERVER_THREADS);
    ioThreads = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_SERVER_IO_THREADS,
                            Constants.Transaction.Service.DEFAULT_DATA_TX_SERVER_IO_THREADS);


    maxReadBufferBytes = conf.getInt(com.continuuity.common.conf.Constants.Thrift.MAX_READ_BUFFER,
                                     com.continuuity.common.conf.Constants.Thrift.DEFAULT_MAX_READ_BUFFER);

    LOG.info("Configuring TransactionService" +
               ", address: " + address +
               ", threads: " + threads +
               ", io threads: " + ioThreads +
               ", max read buffer (bytes): " + maxReadBufferBytes);
  }

  @Override
  protected void doStart() {
    leaderElection = new LeaderElection(zkClient, "/tx.service/leader", new ElectionHandler() {
      @Override
      public void leader() {
        // if the txManager fails, we should stop the server
        InMemoryTransactionManager txManager = txManagerProvider.get();
        txManager.addListener(new ServiceListenerAdapter() {
          @Override
          public void failed(State from, Throwable failure) {
            LOG.error("Transaction manager aborted, stopping transaction service");
            stopAndWait();
          }
        }, MoreExecutors.sameThreadExecutor());

        server = ThriftRPCServer.builder(TTransactionServer.class)
          .setHost(address)
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
          LOG.info("Transaction Thrift Service started successfully on " + server.getBindAddress());
        } catch (Throwable t) {
          LOG.info("Transaction Thrift Service didn't start on " + server.getBindAddress());
          leaderElection.stop();
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
    leaderElection.start();

    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (leaderElection != null) {
      // NOTE: if was a leader this will cause loosing of leadership which in callback above will
      //       de-register service in discovery service and stop the service if needed
      try {
        Uninterruptibles.getUninterruptibly(leaderElection.stop(), 5, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        LOG.warn("Timed out waiting for leader election cancellation to complete");
      } catch (ExecutionException e) {
        LOG.error("Exception when cancelling leader election.", e);
      }
    }

    notifyStopped();
  }
}
