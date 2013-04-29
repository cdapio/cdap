package com.continuuity.performance.benchmark;

public abstract class Agent {
  private final int agentId;

  public Agent(int agentId) {
    this.agentId = agentId;
  }

  public abstract long runOnce(long iteration) throws BenchmarkException;

  public long warmup(int agentId, int numAgents) throws BenchmarkException {
    return 0L;
  }

  public long think(int agentId, int numAgents) throws BenchmarkException {
    return 0L;
  }

  public final int getAgentId() {
    return agentId;
  }
}
