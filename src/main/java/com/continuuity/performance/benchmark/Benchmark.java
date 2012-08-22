package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.io.PrintStream;

public abstract class Benchmark {

  /**
   * List the config properties expected by this benchmark
   */
  public void usage(PrintStream out) {
    out.println("Sorry. This benchmark has no documentation yet.");
  }

  /**
   * Configure this benchmark.
   */
  public void configure(CConfiguration config) throws BenchmarkException {
    // by default do nothing
  }

  /**
   * Initialize the benchmark
   */
  public void initialize() throws BenchmarkException {
    // by default do nothing
  }

  /**
   * Warm up the benchmark. This is done exactly once per benchmark
   */
  public void warmup() throws BenchmarkException {
    // by default do nothing
  }

  /**
   * de-initialize, release all resources
   */
  public void shutdown() throws BenchmarkException {
    // by default do nothing
  }

  /**
   * Yields a list of agent groups, each of which will be run in parallel.
   * @return an array of agent groups
   */
  public abstract AgentGroup[] getAgentGroups();

}
