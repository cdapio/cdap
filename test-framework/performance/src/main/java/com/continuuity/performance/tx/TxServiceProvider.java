package com.continuuity.performance.tx;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.concurrent.TimeUnit;

/**
 * Provider of transaction service.
 */
public class TxServiceProvider extends TxProvider {

  private InMemoryZKServer zookeeper;
  private TransactionService txService;
  private ZKClientService zkClientService;

  @Override
  TransactionSystemClient create() throws BenchmarkException {
    try {
      // start an in-memory zookeeper and remember it in a config object
      zookeeper = InMemoryZKServer.builder().build();
      zookeeper.startAndWait();

      CConfiguration config = CConfiguration.create();
      config.set(Constants.Zookeeper.QUORUM, zookeeper.getConnectionStr());

      // find a free port to use for the service
      int port = PortDetector.findFreePort();
      config.setInt(TxConstants.Service.CFG_DATA_TX_BIND_PORT, port);

      Injector baseInjector = Guice.createInjector (
        new ConfigModule(config),
        new ZKClientModule(),
        new LocationRuntimeModule().getSingleNodeModules(),
        new DiscoveryRuntimeModule().getDistributedModules());

      zkClientService = baseInjector.getInstance(ZKClientService.class);
      zkClientService.startAndWait();

      Injector managerInjector = baseInjector.createChildInjector(new DataFabricModules().getInMemoryModules());
      InMemoryTransactionManager txManager = managerInjector.getInstance(InMemoryTransactionManager.class);
      txManager.startAndWait();


      txService = baseInjector.getInstance(TransactionService.class);
      txService.start();

      TimeUnit.SECONDS.sleep(3);

      // now create a remote tx that connects to the service
      Injector clientInjector = baseInjector.createChildInjector(new DataFabricModules().getDistributedModules());
      return clientInjector.getInstance(TransactionSystemClient.class);
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

    if (zkClientService != null) {
      zkClientService.stopAndWait();
    }

    // and shutdown the zookeeper
    if (zookeeper != null) {
      zookeeper.stopAndWait();
    }
  }
}
