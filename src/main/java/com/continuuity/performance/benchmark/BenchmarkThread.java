package com.continuuity.performance.benchmark;

public class BenchmarkThread extends Thread {

  int agentId;
  AgentGroup agentGroup;
  BenchmarkMetric globalMetrics;

  public BenchmarkThread(AgentGroup group, int agentId,
                         BenchmarkMetric groupMetrics) {
    this.agentGroup = group;
    this.agentId = agentId;
    this.globalMetrics = groupMetrics;
  }

  public void run() {
    Agent agent = agentGroup.newAgent();
    int numAgents = agentGroup.getNumAgents();
    int totalRuns = agentGroup.getTotalRuns() / numAgents;
    int timeToRun = agentGroup.getSecondsToRun();
    int runsPerSecond = agentGroup.getRunsPerSecond();

    System.out.println(agentGroup.getName() + " " + agentId + " starting.");

    long startTime = System.currentTimeMillis();
    long endTime = timeToRun > 0 ? startTime + 1000 * timeToRun : 0;
    long accumulatedTime = 0;

    // for throttling
    long roundStart = System.currentTimeMillis();
    int runsInRound = 0;
    int runs = 0;

    for (; (totalRuns <= 0) || (runs < totalRuns); ++runs) {
      // run one iteration
      long thisTime = System.currentTimeMillis();
      try {
        agent.runOnce(runs + 1, agentId, numAgents);
      } catch (Exception e) {
        // TODO: better way to report the error
        // TODO: add option to continue
        e.printStackTrace();
        break;
      }
      globalMetrics.increment("runs", 1L);

      // if necessary, sleep to throttle runs per second
      long currentTime = System.currentTimeMillis();
      accumulatedTime += currentTime - thisTime;

      if (runsPerSecond > 0) {
        runsInRound++;
        long expectedTime = 1000 * runsInRound / runsPerSecond;
        long roundTime = currentTime - roundStart;
        if (roundTime < expectedTime) {
          try {
            Thread.sleep(expectedTime - roundTime);
          } catch (InterruptedException e) {
            System.out.println("Sleep interrupted. Ignoring.");
          }
          currentTime = System.currentTimeMillis();
        }
        // do we have to start a new round of runsPerSecond?
        if (runsInRound >= runsPerSecond) {
          runsInRound = 0;
          roundStart = currentTime;
        }
      }
      // if time limit is exceeded, break
      if (endTime > 0 && currentTime >= endTime) break;
    }

    long runtime = System.currentTimeMillis() - startTime;
    System.out.println(agentGroup.getName() + " " + agentId + " done: " +
        runs + " runs in " + runtime + " ms, average " +
        (runs * 1000L / runtime) + "/second, average " +
        (accumulatedTime / runs) + " ms/run");
  }

}
