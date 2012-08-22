package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RemoteOpexProvider extends OpexProvider {

  String zkQuorum = null;

  @Override
  public void configure(CConfiguration config) {
    zkQuorum = config.get("zk");
  }

  @Override
  OperationExecutor create() {
    DataFabricDistributedModule module = (DataFabricDistributedModule)
        (new DataFabricModules().getDistributedModules());
    if (zkQuorum != null)
      module.getConfiguration().set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkQuorum);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(OperationExecutor.class);
  }
}
