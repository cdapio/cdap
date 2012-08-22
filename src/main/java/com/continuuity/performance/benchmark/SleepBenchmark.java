package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.Arrays;

public class SleepBenchmark extends SimpleBenchmark {

  int sleep = 10;

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    super.configure(config);
    sleep = config.getInt("sleep", sleep);
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[] {
        new SimpleAgentGroup(super.simpleConfig) {
          @Override
          public String getName() {
            return "sleeper";
          }

          @Override
          public Agent newAgent() {
            return new Agent() {
              @Override
              public void runOnce(int iteration, int agentId, int numAgents)
                  throws BenchmarkException {
                try {
                  if (isVerbose()) {
                    System.out.println(getName() + " " + agentId +
                        " going to sleep for " + sleep + " ms for the " +
                        iteration + ". time." );
                  }
                  Thread.sleep(sleep);
                } catch (InterruptedException e) {
                  System.out.println(getName() + " " + agentId +
                      " interrupted when sleeping for the " + iteration +
                      ". time." );
                }
              }
            };
          } // newAgent()
        } // new AgentGroup()
    }; // new AgentGroup[]
  } // getAgentGroups()


  public static void main(String[] args) {
    args = Arrays.copyOf(args, args.length + 2);
    args[args.length - 2] = "--bench";
    args[args.length - 1] = SleepBenchmark.class.getName();
    BenchmarkRunner.main(args);
  }
}
