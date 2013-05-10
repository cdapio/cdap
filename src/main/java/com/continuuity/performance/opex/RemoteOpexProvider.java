package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * OpexProvider for benchmarks running against a remote data-fabric.
 */
public class RemoteOpexProvider extends OpexProvider {

  String zkQuorum = null;
  String host = null;
  int port = -1;
  int timeout = -1;

  @Override
  public void configure(CConfiguration config) {
    zkQuorum = config.get("zk");
    host = config.get("host");
    port = config.getInt("port", -1);
    timeout = config.getInt("timeout", -1);
  }

  @Override
  OperationExecutor create() {

    DataFabricDistributedModule module = (DataFabricDistributedModule)
        (new DataFabricModules().getDistributedModules());

    if (zkQuorum != null) {
      module.getConfiguration().set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkQuorum);
    }
    if (host != null) {
      module.getConfiguration().set(com.continuuity.data.operation.executor
          .remote.Constants.CFG_DATA_OPEX_SERVER_ADDRESS, host);
      // don't use zookeeper-based service discovery if opex host is given
      module.getConfiguration().unset(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    }
    if (port != -1) {
      module.getConfiguration().setInt(com.continuuity.data.operation.executor
          .remote.Constants.CFG_DATA_OPEX_SERVER_ADDRESS, port);
    }
    if (timeout != -1) {
      module.getConfiguration().setInt(com.continuuity.data.operation.executor
          .remote.Constants.CFG_DATA_OPEX_CLIENT_TIMEOUT, timeout);
    }

    Injector injector = Guice.createInjector(module);
    return injector.getInstance(OperationExecutor.class);
  }
}
