package com.continuuity.performance.opex;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.utils.Bytes;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.Write;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Benchmark implementation that measures Opex write performance.
 */
public class WriteBenchmark extends OpexBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBenchmark.class);

  long doOneWrite(long iteration, int agentId)
      throws BenchmarkException {

    final byte[] key = ("key" + agentId).getBytes();
    final byte[] value = Bytes.toBytes(iteration);
    Write write = new Write(key, Operation.KV_COL, value);

    try {
      opex.commit(opContext, write);
    } catch (OperationException e) {
      LOG.error("Operation {} failed: {}", write, e.getMessage());
      throw new BenchmarkException(
          "Operation " + write + " failed: " + e.getMessage());
    }
    return 1L;
  }

  @Override
  public void warmup() throws BenchmarkException {
    int numWrites = Math.min(100, simpleConfig.numRuns);
    LOG.info("Warmup: Performing {} writes.", numWrites);
    for (int i = 0; i < numWrites; i++) {
      try {
        doOneWrite(i, 0);
      } catch (BenchmarkException e) {
        LOG.error("Failure after {} writes: ", i, e.getMessage());
        throw new BenchmarkException("Failure after " + i + " writes: " + e.getMessage() , e);
      }
    }
    LOG.info("Warmup: Done.");
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[] {
        new SimpleAgentGroup(super.simpleConfig) {
          @Override
          public String getName() {
            return "writer";
          }
          @Override
          public Agent newAgent(final int agentId, int numAgents) {
            return new Agent(agentId) {
              @Override
              public long runOnce(long iteration)
                  throws BenchmarkException {
                return doOneWrite(iteration, agentId);
              }
            };
          } // newAgent()
        } // new SimpleAgentGroup()
    }; // new AgentGroup[]
  } // getAgentGroups()


  public static void main(String[] args) throws Exception {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = WriteBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }

} // WriteBenchmark
