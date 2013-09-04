package com.continuuity.performance.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable for threads that execute benchmark code.
 */
public class BenchmarkRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunnable.class);

  private final int agentId;
  private final AgentGroup agentGroup;
  private final BenchmarkMetric globalMetrics;
  private final boolean useConsole;

  public BenchmarkRunnable(AgentGroup group, int agentId, BenchmarkMetric groupMetrics, boolean useConsole) {
    this.agentGroup = group;
    this.agentId = agentId;
    this.globalMetrics = groupMetrics;
    this.useConsole = useConsole;
  }

  public BenchmarkRunnable(AgentGroup group, int agentId, BenchmarkMetric groupMetrics) {
    this(group, agentId, groupMetrics, true);
  }

  @Override
  public void run() {
    String msg;
    int numAgents = agentGroup.getNumAgents();
    Agent agent = agentGroup.newAgent(agentId, numAgents);
    int totalRuns = agentGroup.getTotalRuns() / numAgents;
    int timeToRun = agentGroup.getSecondsToRun();
    int runsPerSecond = agentGroup.getRunsPerSecond();

    LOG.info("{} {} starting.", agentGroup.getName(), agentId);
    printConsole(String.format("%s %d starting.", agentGroup.getName(), agentId));

    long startTime = System.currentTimeMillis();
    long endTime = timeToRun > 0 ? startTime + 1000 * timeToRun : 0;
    long accumulatedTime = 0;

    // for throttling
    long roundStart = System.currentTimeMillis();
    long runsInRound = 0;
    long runs = 0;

    for (; (totalRuns <= 0) || (runs < totalRuns); ++runs) {
      // run one iteration
      long thisTime = System.currentTimeMillis();
      long delta;

      try {
        delta = agent.runOnce(runs + 1);
      } catch (BenchmarkException e) {
        LOG.error("runOnce failed for run " + (runs + 1) + " of " + agentGroup.getName() + " " + agentId, e);
        throw new RuntimeException("Execution of runOnce failed", e);
      }

      globalMetrics.increment("runs", delta);

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
            Thread.currentThread().interrupt();
            LOG.debug("Sleep interrupted. Ignoring.");
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
      if (endTime > 0 && currentTime >= endTime) {
        break;
      }
    }

    long runtime = System.currentTimeMillis() - startTime;

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s %s done: %d  runs in %d ms", agentGroup.getName(), agentId, runs, runtime));
    if (runtime != 0) {
      sb.append(String.format(", average %d/sec", (runs * 1000L / runtime)));
    }
    if (runs != 0) {
      sb.append(String.format(", average %d ms/run", (accumulatedTime / runs)));
    }
    msg = sb.toString();
    LOG.info(msg);
    printConsole(msg);

    LOG.debug("Benchmark agent thread finished.");
  }

  private void printConsole(String msg) {
    if (useConsole) {
      System.out.println(msg);
    }
  }
}
