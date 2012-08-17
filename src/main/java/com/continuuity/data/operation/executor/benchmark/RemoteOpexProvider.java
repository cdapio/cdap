package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.ArrayList;

public class RemoteOpexProvider extends OpexProvider {

  String zkQuorum = null;

  @Override
  public String[] configure(String[] args) throws BenchmarkException {
    ArrayList<String> remaining = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if ("--zk".equals(args[i])) {
        if (i + 1 < args.length) {
          zkQuorum = args[++i];
        } else {
          throw new BenchmarkException("--zk must have an argument. ");
        }
      } else {
        remaining.add(args[i]);
      }
    }
    return remaining.toArray(new String[remaining.size()]);
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
