package com.continuuity.performance.opex;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import java.util.Arrays;

public class LocalHBaseOpexProvider extends OpexProvider {

  @Override
  OperationExecutor create() throws BenchmarkException {
    try {
      HBaseTestBase.startHBase();
    } catch (Exception e) {
      throw new BenchmarkException(
          "Unable to start HBase: " + e.getMessage(), e);
    }
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor")));
  }

  @Override
  void shutdown(OperationExecutor opex) throws BenchmarkException {
    try {
      HBaseTestBase.stopHBase();
    } catch (Exception e) {
      throw new BenchmarkException(
          "Unable to stop HBase: " + e.getMessage(), e);
    }
  }

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--opex";
    args1[args.length + 1] = LocalHBaseOpexProvider.class.getName();
    BenchmarkRunner.main(args1);
  }
}
