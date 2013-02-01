package com.continuuity.performance.opex;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.Write;
import com.continuuity.performance.benchmark.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class WriteBenchmark extends OpexBenchmark {

  void doOneWrite(long iteration, int agentId)
      throws BenchmarkException {

    final byte[] key = ("key" + agentId).getBytes();
    final byte[] value = Bytes.toBytes(iteration);
    Write write = new Write(key, value);

    try {
      opex.execute(opContext, write);
    } catch (OperationException e) {
      throw new BenchmarkException(
          "Operation " + write + " failed: " + e.getMessage());
    }
  }

  @Override
  public void warmup() throws BenchmarkException {
    int numWrites = Math.min(100, simpleConfig.numRuns);
    System.out.println("Warmup: Performing " + numWrites + " writes.");
    for (int i = 0; i < numWrites; i++) {
      try {
        doOneWrite(i, 0);
      } catch (BenchmarkException e) {
        throw new BenchmarkException(
            "Failure after " + i + " writes: " + e.getMessage() , e);
      }
    }
    System.out.println("Warmup: Done.");
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
          public Agent newAgent() {
            return new Agent() {
              @Override
              public void runOnce(long iteration, int agentId, int numAgents)
                  throws BenchmarkException {
                doOneWrite(iteration, agentId);
              }
            };
          } // newAgent()
        } // new SimpleAgentGroup()
    }; // new AgentGroup[]
  } // getAgentGroups()


  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = WriteBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }

} // WriteBenchmark
