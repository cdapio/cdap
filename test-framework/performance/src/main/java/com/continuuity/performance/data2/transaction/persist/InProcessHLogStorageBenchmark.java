package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.options.Option;
import com.continuuity.common.options.OptionsParser;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.Benchmark;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkMetric;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.continuuity.performance.benchmark.SimpleBenchmark;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs a test of multiple concurrent clients in process with the persistent storage service in order to maximize throughput.
 */
public class InProcessHLogStorageBenchmark extends SimpleBenchmark {
  private static final String APP_NAME = InProcessHLogStorageBenchmark.class.getSimpleName();

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final Logger LOG = LoggerFactory.getLogger(InProcessHLogStorageBenchmark.class);

  private String pathForWAL;
  private int changeSetSize;
  private int batchSize;
  private HLogTransactionStateStorage txStorage;
  private AtomicLong txIdGenerator = new AtomicLong();

  private BenchmarkMetric localMetrics = new BenchmarkMetric();
  private MetricsHistogram latencyMetrics = new MetricsHistogram("HLogStorageBenchmark", null);

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    super.configure(config);
    pathForWAL = config.get("path");
    changeSetSize = config.getInt("size", 1024);
    batchSize = config.getInt("batch", 1);
    if (pathForWAL == null) {
      throw new BenchmarkException("WAL path in HDFS must be given with the --path parameter");
    }
  }

  @Override
  public Map<String,String> usage() {
    Map<String,String> args = super.usage();
    args.put("--path <HDFS>", "Path in HDFS for WAL file storage");
    args.put("--batch <num>", "Number of WAL edits to batch together");
    args.put("--size <bytes>", "Size in bytes of WAL entry payload (for change set states)");
    return args;
  }

  @Override
  public void initialize() throws BenchmarkException {

    Path walPath = new Path(pathForWAL);
    Configuration hConfig = new Configuration();
    txStorage = new HLogTransactionStateStorage(hConfig, walPath);
    try {
      txStorage.init();
    } catch (IOException ioe) {
      throw new BenchmarkException("Failed to initialize HLogTransactionStateStorage", ioe);
    }
  }

  @Override
  public void warmup() throws BenchmarkException {
    super.warmup();
  }

  @Override
  public void shutdown() {
    try {
      txStorage.close();
    } catch (IOException ioe) {
      LOG.error("Failed closing HLog transaction storage", ioe);
    }
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[]{
      new SimpleAgentGroup(simpleConfig) {
        @Override
        public String getName() {
          return APP_NAME;
        }
        @Override
        public Agent newAgent(int agentId, int numAgents) {
          TransactionEditSupplier txSupplier = new TransactionEditSupplier(changeSetSize);
          ClientMetrics metrics = new NoOpClientMetrics(agentId, localMetrics, latencyMetrics);
          return new TransactionClientAgent(agentId, batchSize, txStorage, txSupplier, metrics);
        }

      }
    };  //To change body of implemented methods use File | Settings | File Templates.
  }

  private class TransactionEditSupplier implements Supplier<TransactionEdit> {
    private final Random random = new Random();
    private final Random changeSetRandom = new Random();
    private final int changeSetSize;

    public TransactionEditSupplier(int changeSetSize) {
      this.changeSetSize = changeSetSize;
    }

    @Override
    public TransactionEdit get() {
      int op = random.nextInt(4);
      switch (op) {
        case 0:
          return startTransaction(txIdGenerator.incrementAndGet());
        case 1:
          return committingTransaction(txIdGenerator.incrementAndGet());
        case 2:
          return commitTransaction(txIdGenerator.incrementAndGet());
        case 3:
          return abortTransaction(txIdGenerator.incrementAndGet());
      }
      return null;
    }

    private TransactionEdit startTransaction(long txId) {
      return new TransactionEdit(txId, TransactionEdit.State.INPROGRESS, EMPTY_BYTES);
    }

    private TransactionEdit committingTransaction(long txId) {
      // generate payload
      byte[] payload = new byte[changeSetSize];
      changeSetRandom.nextBytes(payload);
      return new TransactionEdit(txId, TransactionEdit.State.COMMITTING, payload);
    }

    private TransactionEdit commitTransaction(long txId) {
      return new TransactionEdit(txId, TransactionEdit.State.COMMITTED, EMPTY_BYTES);
    }

    private TransactionEdit abortTransaction(long txId) {
      return new TransactionEdit(txId, TransactionEdit.State.INVALID, EMPTY_BYTES);
    }
  }
}
