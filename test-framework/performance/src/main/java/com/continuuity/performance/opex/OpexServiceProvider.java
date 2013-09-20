package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.Constants;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class OpexServiceProvider extends OpexProvider {

  private Injector injector;
  private OperationExecutor opex, remote;
  private InMemoryZookeeper zookeeper;
  private CConfiguration config;
  private OperationExecutorService opexService;

  @Override
  OperationExecutor create() throws BenchmarkException {

    try {
      injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
      injector.getInstance(InMemoryTransactionManager.class).init();
      opex = injector.getInstance(OperationExecutor.class);

      // start an in-memory zookeeper and remember it in a config object
      zookeeper = new InMemoryZookeeper();
      config = CConfiguration.create();
      config.set(Constants.CFG_ZOOKEEPER_ENSEMBLE,
               zookeeper.getConnectionString());

      // find a free port to use for the service
      int port = PortDetector.findFreePort();
      config.setInt(Constants.CFG_DATA_OPEX_SERVER_PORT, port);

      // start an opex service
      opexService = new OperationExecutorService(opex);

      // and start it. Since start is blocking, we have to start async'ly
      new Thread () {
        public void run() {
          try {
            opexService.start(new String[]{}, config);
          } catch (Exception e) {
            System.err.println("Failed to start service: " + e.getMessage());
          }
        }
      }.start();

      TimeUnit.SECONDS.sleep(3);

      // now create a remote opex that connects to the service
      remote = new RemoteOperationExecutor(config);
      return remote;
    } catch (Exception e) {
      throw new BenchmarkException("Failure to start embedded opex service: ", e);
    }
  }

  @Override
  void shutdown(OperationExecutor opex) {
    super.shutdown(opex);

    // shutdown the opex service
    if (opexService != null) {
      opexService.stop(true);
    }

    // and shutdown the zookeeper
    if (zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

}
