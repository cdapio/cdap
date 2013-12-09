package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.Map;
import java.util.TreeMap;

/**
 * Abstract class to be sub-classed by all Benchmark classes.
 */
public abstract class Benchmark {

  /**
   * Configure this benchmark.
   */
  public void configure(CConfiguration config) throws BenchmarkException {
    // by default do nothing
  }

  /**
   * This method is used to inform the command line about the expected options for configure().
   * @return A map of option name to a description of the option.
   */
  public Map<String, String> usage() {
    // by default do configuration options
    return new TreeMap<String, String>();
  }

  /**
   * Initialize the benchmark.
   */
  public void initialize() throws BenchmarkException {
    // by default do nothing
  }

  /**
   * Warm up the benchmark. This is done exactly once per benchmark.
   */
  public void warmup() throws BenchmarkException {
    // by default do nothing
  }

  /**
   * De-initialize, release all resources.
   */
  public void shutdown() {
    // by default do nothing
  }

  /**
   * Yields a list of agent groups, each of which will be run in parallel.
   * @return an array of agent groups
   */
  public abstract AgentGroup[] getAgentGroups();
}


