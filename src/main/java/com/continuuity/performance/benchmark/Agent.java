package com.continuuity.performance.benchmark;

public abstract class Agent {

  public abstract long runOnce(long iteration, int agentId, int numAgents) throws BenchmarkException;

  public long warmup(int agentId, int numAgents) throws BenchmarkException {
    return 0L;
  }

  public long think(int agentId, int numAgents) throws BenchmarkException {
    return 0L;
  }
}
