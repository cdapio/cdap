package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.Map;

/**
 * Simple benchmark.
 */
public abstract class SimpleBenchmark extends Benchmark {

  /**
   * SimpleConfig.
   */
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
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--verbose (true|false)", "To enable verbose output. Default is false.");
    usage.put("--runs <num>", "Total number of runs. Each thread will run runs/threads times. " +
              "Specify 0 for unlimited runs. Default is 0.");
    usage.put("--threads <num>", "Number of threads to run. Default is 1.");
    usage.put("--runtime <seconds>", "To limit the absolute time that the benchmark will run. " +
              "Default is 0 (unlimited).");
    usage.put("--throttle <num>", "To limit the number of runs per second per thread. " +
              "Default is 0 (unlimited).");
    return usage;
  }

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    simpleConfig.configure(config);
  }
}
