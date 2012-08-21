package com.continuuity.common.benchmark;

public abstract class Agent {

  public abstract void runOnce(int iteration, int agentId, int numAgents)
      throws BenchmarkException;

}
