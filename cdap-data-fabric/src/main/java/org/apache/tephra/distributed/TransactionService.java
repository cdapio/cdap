/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.distributed.thrift.TTransactionServer;
import org.apache.tephra.inmemory.InMemoryTransactionService;
import org.apache.tephra.rpc.ThriftRPCServer;
import org.apache.tephra.txprune.TransactionPruningService;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 *
 */
public final class TransactionService extends InMemoryTransactionService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);
  private LeaderElection leaderElection;
  private final Configuration conf;
  private final ZKClient zkClient;

  private ThriftRPCServer<TransactionServiceThriftHandler, TTransactionServer> server;
  private TransactionPruningService pruningService;

  @Inject
  public TransactionService(Configuration conf,
                            ZKClient zkClient,
                            DiscoveryService discoveryService,
                            Provider<TransactionManager> txManagerProvider) {
    super(conf, discoveryService, txManagerProvider);
    this.conf = conf;
    this.zkClient = zkClient;
  }

  @Override
  protected InetSocketAddress getAddress() {
    if (address.equals("0.0.0.0")) {
      // resolve hostname
      try {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), server.getBindAddress().getPort());
      } catch (UnknownHostException x) {
        LOG.error("Cannot resolve hostname for 0.0.0.0", x);
      }
    }
    return server.getBindAddress();
  }

  @Override
  protected void doStart() {
    leaderElection = new LeaderElection(zkClient, "/tx.service/leader", new ElectionHandler() {
      @Override
      public void leader() {
        // if the txManager fails, we should stop the server
        txManager = txManagerProvider.get();
        txManager.addListener(new ServiceListenerAdapter() {
          @Override
          public void failed(State from, Throwable failure) {
            LOG.error("Transaction manager aborted, stopping transaction service");
            TransactionService.this.abort(failure);
          }
        }, MoreExecutors.sameThreadExecutor());

        pruningService = new TransactionPruningService(conf, txManager);

        server = ThriftRPCServer.builder(TTransactionServer.class)
          .setHost(address)
          .setPort(port)
          .setWorkerThreads(threads)
          .setMaxReadBufferBytes(maxReadBufferBytes)
          .setIOThreads(ioThreads)
          .build(new TransactionServiceThriftHandler(txManager));
        try {
          server.startAndWait();
          pruningService.startAndWait();
          doRegister();
          LOG.info("Transaction Thrift Service started successfully on " + getAddress());
        } catch (Throwable t) {
          LOG.info("Transaction Thrift Service didn't start on " + server.getBindAddress());
          leaderElection.stop();
          notifyFailed(t);
        }
      }

      @Override
      public void follower() {
        ListenableFuture<State> stopFuture = null;
        // First stop the transaction server as un-registering from discovery can block sometimes.
        // That can lead to multiple transaction servers being active at the same time.
        if (server != null && server.isRunning()) {
          server.stopAndWait();
        }
        if (pruningService != null && pruningService.isRunning()) {
          // Wait for pruning service to stop after un-registering from discovery
          stopFuture = pruningService.stop();
        }
        undoRegister();

        if (stopFuture != null) {
          Futures.getUnchecked(stopFuture);
        }
      }
    });
    leaderElection.start();

    notifyStarted();
  }

  @VisibleForTesting
  State thriftRPCServerState() {
    return server.state();
  }

  @Override
  protected void doStop() {
    internalStop();
    notifyStopped();
  }

  protected void abort(Throwable cause) {
    // try to clear leader status and shutdown RPC
    internalStop();
    notifyFailed(cause);
  }

  protected void internalStop() {
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
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  @VisibleForTesting
  @Nullable
  public TransactionManager getTransactionManager() {
    return txManager;
  }
}
