package com.continuuity.performance.benchmark;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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

    Stopwatch totalTimer = new Stopwatch().start();
    long accumulatedTime = 0;

    // for throttling
    Stopwatch roundTimer = new Stopwatch().start();
    long runsInRound = 0;
    long runs = 0;

    Stopwatch runTimer = new Stopwatch();

    for (; (totalRuns <= 0) || (runs < totalRuns); ++runs) {
      // run one iteration
      runTimer.start();
      long delta;

      try {
        delta = agent.runOnce(runs + 1);
      } catch (BenchmarkException e) {
        LOG.error("runOnce failed for run " + (runs + 1) + " of " + agentGroup.getName() + " " + agentId, e);
        throw new RuntimeException("Execution of runOnce failed", e);
      }

      globalMetrics.increment("runs", delta);

      // if necessary, sleep to throttle runs per second
      accumulatedTime += runTimer.elapsedTime(TimeUnit.MILLISECONDS);
      runTimer.reset();

      if (runsPerSecond > 0) {
        runsInRound++;
        long expectedTime = 1000 * runsInRound / runsPerSecond;
        long roundTime = roundTimer.elapsedTime(TimeUnit.MILLISECONDS);
        if (roundTime < expectedTime) {
          try {
            Thread.sleep(expectedTime - roundTime);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("Sleep interrupted. Ignoring.");
          }
        }
        // do we have to start a new round of runsPerSecond?
        if (runsInRound >= runsPerSecond) {
          runsInRound = 0;
          roundTimer.reset().start();
        }
      }
      // if time limit is exceeded, break
      if (timeToRun > 0 && totalTimer.elapsedTime(TimeUnit.SECONDS) > timeToRun) {
        break;
      }
    }

    long runtime = totalTimer.elapsedTime(TimeUnit.MILLISECONDS);

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s %s done: %d  runs in %s", agentGroup.getName(), agentId, runs, totalTimer));
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
