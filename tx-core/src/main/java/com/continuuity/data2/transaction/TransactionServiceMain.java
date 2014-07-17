/*
 * Copyright 2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.data2.transaction;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data2.transaction.distributed.PooledClientProvider;
import com.continuuity.data2.transaction.distributed.ThreadLocalClientProvider;
import com.continuuity.data2.transaction.distributed.ThriftClientProvider;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.transaction.runtime.TransactionDistributedModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.twill.common.Services;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop tx in distributed mode.
 */
public class TransactionServiceMain {

  private CConfiguration cConf = null;

  private TransactionService txService = null;

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceMain.class);

  public static void main(String args[]) throws Exception {
    TransactionServiceMain instance = new TransactionServiceMain();
    instance.doMain(args);
  }

  /**
   * The main method. It simply call methods in the same sequence
   * as if the program is started by jsvc.
   */
  public void doMain(final String[] args) throws Exception {
    final CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
      try {
        try {
          TransactionServiceMain.this.stop();
        } finally {
          try {
            TransactionServiceMain.this.destroy();
          } finally {
            shutdownLatch.countDown();
          }
        }
      } catch (Throwable t) {
        LOG.error("Exception when shutting down: " + t.getMessage(), t);
      }
      }
    });
    init(args);
    start();

    shutdownLatch.await();
  }

  /**
   * Invoked by jsvc to initialize the program.
   */
  public void init(String[] args) {
    cConf = CConfiguration.create();
  }

  /**
   * Invoked by jsvc to start the program.
   */
  public void start() throws Exception {
    final ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
              .setSessionTimeout(cConf.getInt(
                Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
            RetryStrategies.fixDelay(2, TimeUnit.MILLISECONDS)
          )
        )
      );

    zkClientService.start();

    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionDistributedModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ZKClient.class).toInstance(zkClientService);
          bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
        }
      }
    );

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    Future<?> future = Services.getCompletionFuture(txService);
    try {
      txService.start();
    } catch (Exception e) {
      System.err.println("Failed to start service: " + e.getMessage());
    }
    future.get();
  }

  /**
   * Invoked by jsvc to stop the program.
   */
  public void stop() {
    if (txService == null) {
      return;
    }
    try {
      if (txService.isRunning()) {
        txService.stopAndWait();
      }
    } catch (Throwable e) {
      LOG.error("Failed to shutdown transaction service.", e);
      // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
      System.err.println("Failed to shutdown transaction service: " + e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  /**
   * Invoked by jsvc for resource cleanup.
   */
  public void destroy() { }

  /**
   * Provides implementation of {@link com.continuuity.data2.transaction.distributed.ThriftClientProvider}
   * based on configuration.
   */
  @Singleton
  private static final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

    private final CConfiguration cConf;
    private DiscoveryServiceClient discoveryServiceClient;

    @Inject
    ThriftClientProviderSupplier(CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Inject(optional = true)
    void setDiscoveryServiceClient(DiscoveryServiceClient discoveryServiceClient) {
      this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public ThriftClientProvider get() {
      // configure the client provider
      String provider = cConf.get(TxConstants.Service.CFG_DATA_TX_CLIENT_PROVIDER,
                                  TxConstants.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
      ThriftClientProvider clientProvider;
      if ("pool".equals(provider)) {
        clientProvider = new PooledClientProvider(cConf, discoveryServiceClient);
      } else if ("thread-local".equals(provider)) {
        clientProvider = new ThreadLocalClientProvider(cConf, discoveryServiceClient);
      } else {
        String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }
}
