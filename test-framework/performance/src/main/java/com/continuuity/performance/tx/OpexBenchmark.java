package com.continuuity.performance.tx;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TransactionSystemClient;
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

  TxProvider txProvider;
  TransactionSystemClient txClient;

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--tx <name>", "To specify the tx service to use. " +
        "Valid short values are 'memory', 'remote' and 'hbase'. " +
        "Alternatively, specify the name of a class that implements " +
        "TxProvider, and its create() method will be used to obtain the " +
        "tx service/client.");
    usage.put("--zk", "For some tx providers, specifies the zookeeper " +
        "quorum to use.");
    return usage;
  }

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {

    // first configure all the standard stuff
    super.configure(config);

    // now try to figure out the operation executor
    String txName = config.get("tx");
    if (txName == null) {
      throw new BenchmarkException("--tx must be specified.");
    }
    if ("memory".equals(txName)) {
      this.txProvider = new InMemoryTxProvider();
    } else if ("hbase".equals(txName)) {
      this.txProvider = new HBaseTxProvider();
    } else if ("remote".equals(txName)) {
      this.txProvider = new RemoteTxProvider();
    } else if ("service".equals(txName)) {
      this.txProvider = new TxServiceProvider();
    } else if ("noop".equals(txName)) {
      this.txProvider = new MinimalTxProvider();
    } else {
      // consider txName the class name of an tx provider
      // if it is not a fully qualified class name, add package to it
      if (!txName.startsWith("com.continuuity")) {
        txName = this.getClass().getPackage().getName() + "." + txName;
      }
      try {
        this.txProvider = (TxProvider) Class.forName(txName).newInstance();
      } catch (Exception e) {
        throw new BenchmarkException(
            "Cannot instantiate tx provider '" + txName + "': " +
                e.getMessage());
      }
    }
    this.txProvider.configure(config);
  }

  @Override
  public void initialize() throws BenchmarkException {
    super.initialize();
    this.txClient = this.txProvider.create();
  }

  @Override
  public void shutdown() {
    LOG.debug("Shutting down tx provider.");
    if (this.txClient != null) {
      this.txProvider.shutdown(this.txClient);
    }
  }
}
