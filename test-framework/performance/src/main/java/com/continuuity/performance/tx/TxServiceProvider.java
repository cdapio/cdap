package com.continuuity.performance.tx;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.concurrent.TimeUnit;

/**
 * Provider of transaction service.
 */
public class TxServiceProvider extends TxProvider {

  private Injector injector;
  private InMemoryZookeeper zookeeper;
  private TransactionService txService;

  @Override
  TransactionSystemClient create() throws BenchmarkException {

    try {
      CConfiguration config = CConfiguration.create();
      config.set(Constants.Zookeeper.QUORUM,
                 zookeeper.getConnectionString());

      // find a free port to use for the service
      int port = PortDetector.findFreePort();
      config.setInt(Constants.Transaction.Service.CFG_DATA_TX_BIND_PORT, port);

      ZKClientService zkClientService = getZkClientService(config);
      zkClientService.start();

      injector = Guice.createInjector (
        new DataFabricModules(config).getInMemoryModules(),
        new DiscoveryRuntimeModule(zkClientService).getDistributedModules());

      InMemoryTransactionManager txManager = injector.getInstance(InMemoryTransactionManager.class);
      txManager.startAndWait();

      // start an in-memory zookeeper and remember it in a config object
      zookeeper = new InMemoryZookeeper();


      txService = injector.getInstance(TransactionService.class);
      Thread t = new Thread() {
        @Override
        public void run() {
          txService.start();
        }
      };
      t.start();

      // and start it. Since start is blocking, we have to start async'ly
      new Thread () {
        public void run() {
          try {
            txService.start();
          } catch (Exception e) {
            System.err.println("Failed to start service: " + e.getMessage());
          }
        }
      }.start();

      TimeUnit.SECONDS.sleep(3);

      // now create a remote tx that connects to the service
      return new TransactionServiceClient(config);
    } catch (Exception e) {
      throw new BenchmarkException("error init'ing txSystemClient", e);
    }
  }

  @Override
  void shutdown(TransactionSystemClient opex) {
    super.shutdown(opex);

    // shutdown the tx service
    if (txService != null) {
      txService.stop();
    }

    // and shutdown the zookeeper
    if (zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  private static ZKClientService getZkClientService(CConfiguration conf) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(conf.get(com.continuuity.common.conf.Constants.Zookeeper.QUORUM))
          .setSessionTimeout(conf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                         Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
          .build(), RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
        )
      )
    );
  }

}
