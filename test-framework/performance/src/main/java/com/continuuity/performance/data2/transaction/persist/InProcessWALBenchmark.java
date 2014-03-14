package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionEdit;
import com.continuuity.data2.transaction.persist.TransactionLog;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkMetric;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.continuuity.performance.benchmark.SimpleBenchmark;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import org.apache.hadoop.metrics.MetricsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Runs a test of multiple concurrent clients in process with the persistent storage service in order to
 * maximize throughput.
 */
public class InProcessWALBenchmark extends SimpleBenchmark {
  private static final String APP_NAME = InProcessWALBenchmark.class.getSimpleName();

  private static final Logger LOG = LoggerFactory.getLogger(InProcessWALBenchmark.class);

  private String pathForWAL;
  private int batchSize;
  private int changeSetSize;
  private HDFSTransactionStateStorage txStorage;
  private TransactionLog log;

  private BenchmarkMetric localMetrics = new BenchmarkMetric();
  private MetricsHistogram latencyMetrics = new MetricsHistogram("HLogStorageBenchmark", null);

  private List<TransactionClientAgent> agents = Lists.newArrayList();

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    super.configure(config);
    pathForWAL = config.get("path");
    batchSize = config.getInt("batch", 1);
    changeSetSize = config.getInt("changeSetSize", 10);
    if (pathForWAL == null) {
      throw new BenchmarkException("WAL path in HDFS must be given with the --path parameter");
    }
  }

  @Override
  public Map<String, String> usage() {
    Map<String, String> args = super.usage();
    args.put("--path <HDFS>", "Path in HDFS for WAL file storage");
    args.put("--batch <num>", "Number of WAL edits to batch together");
    args.put("--changeSetSize <num>", "Number of change set entries (only applies to committing and committed tx)");
    return args;
  }

  @Override
  public void initialize() throws BenchmarkException {

    Path walPath = new Path(pathForWAL);
    CConfiguration conf = CConfiguration.create();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, walPath.toString());
    Configuration hConfig = new Configuration();
    txStorage = new HDFSTransactionStateStorage(conf, hConfig);
    txStorage.startAndWait();
    try {
      log = txStorage.createLog(System.currentTimeMillis());
    } catch (IOException ioe) {
      throw new BenchmarkException("Error creating write-ahead log", ioe);
    }
  }

  @Override
  public void warmup() throws BenchmarkException {
    super.warmup();
    // do a dummy append to make sure the log is open
    try {
      log.append(TransactionEdit.createMoveWatermark(1));
    } catch (IOException ioe) {
      throw new BenchmarkException("Error warming up transaction log", ioe);
    }
  }

  @Override
  public void shutdown() {
    try {
      log.close();
    } catch (IOException ioe) {
      LOG.error("Error closing transaction log", ioe);
    }
    txStorage.stopAndWait();

    // print out client metrics
    for (TransactionClientAgent agent : agents) {
      ClientMetrics metrics = agent.getMetrics();
      long successCount = metrics.getSuccessCount();
      long failedCount = metrics.getFailureCount();
      Stopwatch totalTimer = metrics.getTotalTimer();
      LOG.info(String.format("Agent %s: succeeded / failed = %d / %d, total time = %s, ops / sec = %5.2f",
                             agent.getAgentId(), successCount, failedCount, totalTimer,
                             ((successCount + failedCount) / (float) totalTimer.elapsedTime(TimeUnit.SECONDS))));
    }

    StringBuilderMetricsRecord metricsRecord = new StringBuilderMetricsRecord();
    latencyMetrics.pushMetric(metricsRecord);
    LOG.info("All client metrics: " + metricsRecord.toString());
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
          ClientMetrics metrics = new ClientMetrics(agentId, localMetrics, latencyMetrics);
          TransactionClientAgent agent = new TransactionClientAgent(agentId, batchSize, log, txSupplier, metrics);
          agents.add(agent);
          return agent;
        }

      }
    };  //To change body of implemented methods use File | Settings | File Templates.
  }

  private class TransactionEditSupplier implements Supplier<TransactionEdit> {
    private final Random random = new Random();
    private final int changeSetSize;

    public TransactionEditSupplier(int changeSetSize) {
      this.changeSetSize = changeSetSize;
    }

    @Override
    public TransactionEdit get() {
      return createRandomEdits(1).get(0);
    }

    private Set<ChangeId> generateChangeSet(int numEntries) {
      Set<ChangeId> changes = Sets.newHashSet();
      for (int i = 0; i < numEntries; i++) {
        byte[] bytes = new byte[8];
        random.nextBytes(bytes);
        changes.add(new ChangeId(bytes));
      }
      return changes;
    }

    /**
     * Generates a number of semi-random {@link TransactionEdit} instances.  These are just randomly selected from the
     * possible states, so would not necessarily reflect a real-world distribution.
     *
     * @param numEntries how many entries to generate in the returned list.
     * @return a list of randomly generated transaction log edits.
     */
    private List<TransactionEdit> createRandomEdits(int numEntries) {
      List<TransactionEdit> edits = Lists.newArrayListWithCapacity(numEntries);
      for (int i = 0; i < numEntries; i++) {
        TransactionEdit.State nextType = TransactionEdit.State.values()[random.nextInt(6)];
        long writePointer = Math.abs(random.nextLong());
        switch (nextType) {
          case INPROGRESS:
            edits.add(
              TransactionEdit.createStarted(writePointer, writePointer - 1,
                                            System.currentTimeMillis() + 300000L, writePointer + 1));
            break;
          case COMMITTING:
            edits.add(TransactionEdit.createCommitting(writePointer, generateChangeSet(changeSetSize)));
            break;
          case COMMITTED:
            edits.add(TransactionEdit.createCommitted(writePointer, generateChangeSet(changeSetSize), writePointer + 1,
                                                      random.nextBoolean()));
            break;
          case INVALID:
            edits.add(TransactionEdit.createInvalid(writePointer));
            break;
          case ABORTED:
            edits.add(TransactionEdit.createAborted(writePointer));
            break;
          case MOVE_WATERMARK:
            edits.add(TransactionEdit.createMoveWatermark(writePointer));
            break;
        }
      }
      return edits;
    }
  }

  private static class StringBuilderMetricsRecord implements MetricsRecord {
    private StringBuilder buf = new StringBuilder();

    @Override
    public String getRecordName() {
      return null;
    }

    @Override
    public void setTag(String tagName, String tagValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("tag: ").append(tagName).append("=").append(tagValue);
    }

    @Override
    public void setTag(String tagName, int tagValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("tag: ").append(tagName).append("=").append(tagValue);
    }

    @Override
    public void setTag(String tagName, long tagValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("tag: ").append(tagName).append("=").append(tagValue);
    }

    @Override
    public void setTag(String tagName, short tagValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("tag: ").append(tagName).append("=").append(tagValue);
    }

    @Override
    public void setTag(String tagName, byte tagValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("tag: ").append(tagName).append("=").append(tagValue);
    }

    @Override
    public void removeTag(String tagName) {
    }

    @Override
    public void setMetric(String metricName, int metricValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("metric: ").append(metricName).append("=").append(metricValue);
    }

    @Override
    public void setMetric(String metricName, long metricValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("metric: ").append(metricName).append("=").append(metricValue);
    }

    @Override
    public void setMetric(String metricName, short metricValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("metric: ").append(metricName).append("=").append(metricValue);
    }

    @Override
    public void setMetric(String metricName, byte metricValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("metric: ").append(metricName).append("=").append(metricValue);
    }

    @Override
    public void setMetric(String metricName, float metricValue) {
      if (buf.length() > 0) {
        buf.append(", ");
      }
      buf.append("metric: ").append(metricName).append("=").append(metricValue);
    }

    @Override
    public void incrMetric(String metricName, int metricValue) {
    }

    @Override
    public void incrMetric(String metricName, long metricValue) {
    }

    @Override
    public void incrMetric(String metricName, short metricValue) {
    }

    @Override
    public void incrMetric(String metricName, byte metricValue) {
    }

    @Override
    public void incrMetric(String metricName, float metricValue) {
    }

    @Override
    public void update() {
    }

    @Override
    public void remove() {
    }

    public String toString() {
      return buf.toString();
    }
  }
}
