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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.twill.common.Services;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop tx in distributed mode.
 */
public class TransactionServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceMain.class);

  private static final int START = 1;
  private static final int STOP = 2;

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = TransactionServiceMain.class.getSimpleName();
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }

  public static void main(String args[]) throws Exception {

    if (args.length != 1) {
      usage(true);
      return;
    }
    if ("--help".equals(args[0])) {
      usage(false);
      return;
    }

    int command;

    if ("start".equals(args[0])) {
      command = START;
    } else if ("stop".equals(args[0])) {
      command = STOP;
    } else {
      usage(true);
      return;
    }

    CConfiguration cConf = CConfiguration.create();

    final ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
              .setSessionTimeout(cConf.getInt(
                Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
            org.apache.twill.zookeeper.RetryStrategies.fixDelay(2, TimeUnit.MILLISECONDS)
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
      });

    // start a tx server
    final TransactionService txService = injector.getInstance(TransactionService.class);

    if (START == command) {
      ShutdownHookManager.get().addShutdownHook(new Thread() {
        @Override
        public void run() {
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
      }, FileSystem.SHUTDOWN_HOOK_PRIORITY + 1);

      Future<?> future = Services.getCompletionFuture(txService);
      try {
        txService.start();
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
      }
      future.get();
    } else {
      System.out.println("Stopping Transaction Service...");
      txService.stop();
    }
  }

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
