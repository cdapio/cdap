package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

public abstract class SimpleBenchmark extends Benchmark {

  public class SimpleConfig {
    public boolean verbose = false;
    public int numRuns = 0;
    public int numAgents = 1;
    public int numSeconds = 0;
    public int numRunsPerSecond = 0;

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
