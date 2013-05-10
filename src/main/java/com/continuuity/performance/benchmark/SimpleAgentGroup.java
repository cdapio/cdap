package com.continuuity.performance.benchmark;

/**
 * Group of simple agents.
 */
public abstract class SimpleAgentGroup extends AgentGroup {

  SimpleBenchmark.SimpleConfig config;

  public SimpleAgentGroup(SimpleBenchmark.SimpleConfig config) {
    this.config = config;
  }

  @Override
  public int getRunsPerSecond() {
    return config.numRunsPerSecond;
  }

  @Override
  public int getTotalRuns() {
    return config.numRuns;
  }

  @Override
  public int getSecondsToRun() {
    return config.numSeconds;
  }

  @Override
  public int getNumAgents() {
    return config.numAgents;
  }

  public boolean isVerbose() {
    return config.verbose;
  }
}

