package com.continuuity.performance.benchmark;

/**
 * Group of agents that execute the same benchmark code.
 */
public abstract class AgentGroup {
  /**
   * Returns the number of times each agent should be called per second.
   * @return a positive integer, or zero for unbounded calls.
   */
  public int getRunsPerSecond() {
    return 0;
  }

  /**
   * The total number of times each agent should be called.
   * @return a positive integer, or zero for unbounded number of calls
   */
  public int getTotalRuns() {
    return 1;
  }

  /**
   * The number of seconds an agent should run.
   * @return a positive number, or zero for unbounded runtime
   */
  public int getSecondsToRun() {
    return 0;
  }

  /**
   * The name of this agent group, to use in statistics and progress.
   */
  public abstract String getName();

  /**
   * The number of concurrent agents to run. All agents run with the same
   * configuration.
   * @return a positive number of agents to run
   */
  public int getNumAgents() {
    return 1;
  }

  /**
   * Create a new agent that is ready to run.
   * @return a new agent
   */
  public abstract Agent newAgent(final int agentId, final int numAgents);
}
