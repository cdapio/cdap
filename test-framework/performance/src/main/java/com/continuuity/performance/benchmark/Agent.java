package com.continuuity.performance.benchmark;

/**
 * Agent that executes benchmark code.
 */
public abstract class Agent {
  private final int agentId;

  public Agent(int agentId) {
    this.agentId = agentId;
  }

  protected abstract long runOnce(long iteration) throws BenchmarkException;

  public final int getAgentId() {
    return agentId;
  }
}
