package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.SimpleBenchmark;

public abstract class OpexBenchmark extends SimpleBenchmark {

  OpexProvider opexProvider;
  OperationExecutor opex;

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {

    // first configure all the standard stuff
    super.configure(config);

    // now try to figure out the operation executor
    String opexName = config.get("opex");
    if (opexName == null) {
      throw new BenchmarkException("--opex must be specified.");
    }
    if ("memory".equals(opexName)) {
      this.opexProvider = new MemoryOpexProvider();
    } else if ("hbase".equals(opexName)) {
      this.opexProvider = new HBaseOpexProvider();
    } else if ("remote".equals(opexName)) {
      this.opexProvider = new RemoteOpexProvider();
    } else {
      // consider opexName the class name of an opex provider
      // if it is not a fully qualified class name, add package to it
      if (!opexName.startsWith("com.continuuity"))
        opexName = this.getClass().getPackage().getName() + "." + opexName;
      try {
        this.opexProvider =
            (OpexProvider)Class.forName(opexName).newInstance();
      } catch (Exception e) {
        throw new BenchmarkException(
            "Cannot instantiate opex provider '" + opexName + "': " +
                e.getMessage());
      }
      this.opexProvider.configure(config);
    }
  }

  @Override
  public void initialize() throws BenchmarkException {
    super.initialize();
    this.opex = this.opexProvider.create();
  }

  @Override
  public void shutdown() throws BenchmarkException {
    this.opexProvider.shutdown(this.opex);
  }

}
