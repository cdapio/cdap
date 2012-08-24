package com.continuuity.performance.benchmark;

public abstract class Agent {

  public abstract void runOnce(long iteration, int agentId, int numAgents)
      throws BenchmarkException;

}
