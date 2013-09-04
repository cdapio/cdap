package com.continuuity.performance.opex;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.google.common.collect.Lists;
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
                Transaction tx = opex.start();
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
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = TxBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }
}
