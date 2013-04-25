package com.continuuity.performance.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkThread extends Thread {

  Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

  Thread runnerThread;
  int agentId;
  AgentGroup agentGroup;
  BenchmarkMetric globalMetrics;
  boolean useConsole;

  public BenchmarkThread(Thread runnerThread,
                         AgentGroup group,
                         int agentId,
                         BenchmarkMetric groupMetrics,
                         boolean useConsole) {
    this.runnerThread = runnerThread;
    this.agentGroup = group;
    this.agentId = agentId;
    this.globalMetrics = groupMetrics;
    this.useConsole = useConsole;
  }

  public BenchmarkThread(Thread runnerThread,
                         AgentGroup group,
                         int agentId,
                         BenchmarkMetric groupMetrics) {
    this(runnerThread, group, agentId, groupMetrics, true);
  }

  public void run() {
    String msg;
    Agent agent = agentGroup.newAgent(agentId);
    int numAgents = agentGroup.getNumAgents();
    int totalRuns = agentGroup.getTotalRuns() / numAgents;
    int timeToRun = agentGroup.getSecondsToRun();
    int runsPerSecond = agentGroup.getRunsPerSecond();

    msg = String.format("%s %d  warming up.", agentGroup.getName(), agentId);
    LOG.info(msg);
    printConsole(msg);

    try {
      agent.warmup(agentId, numAgents);
    } catch (BenchmarkException e) {
      LOG.error(e.getMessage());
    }

    msg = String.format("%s %d starting.", agentGroup.getName(), agentId);
    LOG.info(msg);
    printConsole(msg);

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
        delta = agent.runOnce(runs + 1, agentId, numAgents);
      } catch (BenchmarkException e) {
        // TODO: better way to report the error
        // TODO: add option to continue
        LOG.error(e.getMessage());
        break;
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
            LOG.info("Sleep interrupted. Ignoring.");
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

    LOG.debug("Notify BenchmarkRunner thread about completion.");
    runnerThread.interrupt();
  }

  private void printConsole(String msg) {
    if (useConsole) {
      System.out.println(msg);
    }
  }
}
