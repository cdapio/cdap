package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.SimpleBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract class for benchmarks that use Opex.
 */
public abstract class OpexBenchmark extends SimpleBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(OpexBenchmark.class);

  OpexProvider opexProvider;
  OperationExecutor opex;
  OperationContext opContext = new OperationContext("benchmark");

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--opex <name>", "To specify the operation executor to use. " +
        "Valid short values are 'memory', 'remote' and 'hbase'. " +
        "Alternatively, specify the name of a class that implements " +
        "OpexProvider, and its create() method will be used to obtain the " +
        "opex.");
    usage.put("--zk", "For some opex providers, specifies the zookeeper " +
        "quorum to use.");
    return usage;
  }

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
    } else if ("service".equals(opexName)) {
      this.opexProvider = new OpexServiceProvider();
    } else if ("noop".equals(opexName)) {
      this.opexProvider = new NoOpexProvider();
    } else {
      // consider opexName the class name of an opex provider
      // if it is not a fully qualified class name, add package to it
      if (!opexName.startsWith("com.continuuity")) {
        opexName = this.getClass().getPackage().getName() + "." + opexName;
      }
      try {
        this.opexProvider = (OpexProvider) Class.forName(opexName).newInstance();
      } catch (Exception e) {
        throw new BenchmarkException(
            "Cannot instantiate opex provider '" + opexName + "': " +
                e.getMessage());
      }
    }
    this.opexProvider.configure(config);
  }

  @Override
  public void initialize() throws BenchmarkException {
    super.initialize();
    this.opex = this.opexProvider.create();
  }

  @Override
  public void shutdown() {
    LOG.debug("Shutting down opex provider.");
    if (this.opex != null) {
      this.opexProvider.shutdown(this.opex);
    }
  }
}
