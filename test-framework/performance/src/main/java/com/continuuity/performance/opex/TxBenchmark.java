package com.continuuity.performance.opex;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkResult;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.google.common.collect.Lists;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests the performance of concurrent transaction clients.
 */
public class TxBenchmark extends OpexBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(QueueBenchmark.class);

  private int txSize = 10;
  private int sleep = 0;
  private int numThreads = 10;

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--threads <num>", "Number of threads agents to run. Default is 10. ");
    usage.put("--txsize <num>", "Number of changes per transaction. ");
    usage.put("--sleep <num>", "Number of microseconds to sleep in each transaction, 0 to yield, " +
      "-1 to keep going. Default is 0.");
    return usage;
  }

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    // configure everything else
    super.configure(config);

    // get number of tx clients
    numThreads = config.getInt("threads", numThreads);
    // get tx size
    txSize = config.getInt("txsize", txSize);
    // get sleep time
    sleep = config.getInt("sleep", sleep);
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[] {

      new SimpleAgentGroup(super.simpleConfig) {
        @Override
        public String getName() {
          return "txclient";
        }
        @Override
        public int getNumAgents() {
          return numThreads;
        }
        @Override
        public Agent newAgent(final int agentId, final int numAgents) {
          return new Agent(agentId) {
            ArrayList<byte[]> changes = randomChanges();

            ArrayList<byte[]> randomChanges() {
              ArrayList<byte[]> list = Lists.newArrayList();
              Random rand = new Random(agentId);
              for (int i = 0; i < txSize; i++) {
                list.add(Bytes.add(Bytes.toBytes(agentId), Bytes.toBytes(rand.nextLong())));
              }
              return list;
            }

            @Override
            public long runOnce(long iteration) throws BenchmarkException {
              try {
                Transaction tx = opex.startShort();
                if (sleep > 0) {
                  TimeUnit.MICROSECONDS.sleep(sleep);
                } else if (sleep == 0) {
                  Thread.yield();
                }
                if (opex.canCommit(tx, changes)) {
                  if (!opex.commit(tx)) {
                    LOG.info("Transaction failed.");
                    opex.abort(tx);
                  }
                }
                return 1;
              } catch (Exception e) {
                throw new BenchmarkException("Client " + agentId + " failure: " + e.getMessage(), e);
              }
            }
          };
        } // newAgent()
      }, // new SimpleAgentGroup()
    }; // new AgentGroup[]
  }

  public static void main(String[] args) throws Exception {
    args = Arrays.copyOf(args, args.length + 2);
    args[args.length - 2] = "--bench";
    args[args.length - 1] = TxBenchmark.class.getName();
    final int[] threadsVariation = { 1, 5, 10, 100, 500 };
    final int[] sizeVariation = { 0, 1, 2, 10, 100 };
    double[][] throughputs = new double[threadsVariation.length][sizeVariation.length];
    for (int i = 0; i < threadsVariation.length; i++) {
      int threads = threadsVariation[i];
      String args1[];
      if (threads == 1) {
        args1 = Arrays.copyOf(args, args.length + 4);
        args1[args.length] = "--sleep";
        args1[args.length + 1] = "-1";
      } else {
        args1 = Arrays.copyOf(args, args.length + 2);
      }
      args1[args1.length - 2] = "--threads";
      args1[args1.length - 1] = Integer.toString(threads);
      for (int j = 0; j < sizeVariation.length; j++) {
        int size = sizeVariation[j];
        String[] args2 = Arrays.copyOf(args1, args1.length + 2);
        args2[args1.length] = "--txsize";
        args2[args1.length + 1] = Integer.toString(size);
        BenchmarkResult result = new BenchmarkRunner().doRun(args2);
        double runsPerSecond = result.getGroupResults().iterator().next().getMetrics().get("runs")
          * 1000.0 / result.getRuntimeMillis();
        throughputs[i][j] = runsPerSecond;
      }
    }
    System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(throughputs));
    for (int i = 0; i < threadsVariation.length; i++) {
      for (int j = 0; j < sizeVariation.length; j++) {
        System.out.println(String.format("%d threads with tx size %d: %1.1f/sec",
                                         threadsVariation[i], sizeVariation[j], throughputs[i][j]));
      }
    }
    LOG.info("Benchmark executed successfully.");
  }
}
