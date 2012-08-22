package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

public abstract class SimpleBenchmark extends Benchmark {

  public class SimpleConfig {
    public boolean verbose = false;
    int numRuns = 0;
    int numAgents = 1;
    int numSeconds = 0;
    int numRunsPerSecond = 0;

    public void configure(CConfiguration config) {
      verbose = config.getBoolean("verbose", verbose);
      numRuns = config.getInt("runs", numRuns);
      numAgents = config.getInt("threads", numAgents);
      numSeconds = config.getInt("runtime", numSeconds);
      numRunsPerSecond = config.getInt("throttle", numRunsPerSecond);
    }
  }

  protected SimpleConfig simpleConfig = new SimpleConfig();

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    simpleConfig.configure(config);
  }
}
