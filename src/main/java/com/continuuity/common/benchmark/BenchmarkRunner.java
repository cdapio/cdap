package com.continuuity.common.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.LinkedList;

public class BenchmarkRunner {

  String benchName = null;
  Benchmark benchmark = null;
  CConfiguration config = CConfiguration.create();

  static void error(String message) {
    System.err.println("Error: " + message);
  }

  static void usage() {
    System.out.println("Usage: BenchmarkRunner --bench <name> [ --<key> " +
        "<value> ... ]");
  }

  boolean parseOptions(String[] args) throws BenchmarkException {
    // 1. parse command line for --bench, copy everything else into config
    for (int i = 0; i < args.length; i++) {
      if ("--help".equals(args[i])) {
        usage();
        return false;
      }
      else if (args[i].startsWith("--")) {
        if (i + 1 < args.length) {
          String key = args[i].substring(2);
          String value = args[++i];
          config.set(key, value);
          if ("bench".equals(key))
            benchName = value;
        } else {
          throw new BenchmarkException("--<key> must have an argument.");
        }
      }
    }

    // 2. instantiate benchmark and configure it
    if (benchName == null) {
      throw new BenchmarkException("--bench must be specified.");
    }
    if (!benchName.startsWith("com.continuuity")) {
      benchName = this.getClass().getPackage().getName() + "." + benchName;
    }
    try {
      benchmark = (Benchmark)Class.forName(benchName).newInstance();
    } catch (Exception e) {
      throw new BenchmarkException("Unable to intsantiate benchmark '" +
          benchName + "': " + e.getMessage(), e);
    }
    benchmark.configure(config);
    return true;
  }

  boolean run() throws BenchmarkException {
    // 1. initialize benchmark
    benchmark.initialize();

    // 2. warm up benchmark
    benchmark.warmup();

    // 3. get agent groups and create a thread for each agent
    AgentGroup[] groups = benchmark.getAgentGroups();
    BenchmarkMetric[] groupMetrics = new BenchmarkMetric[groups.length];
    LinkedList<BenchmarkThread> threadList = new LinkedList<BenchmarkThread>();
    for (int j = 0; j < groups.length; j++) {
      AgentGroup group = groups[j];
      System.out.println("Running " + group.getNumAgents() + " " +
          group.getName() + " agents (" + (group.getTotalRuns() > 0 ?
          Integer.toString(group.getTotalRuns()) : "unlimited") + " runs, " +
          (group.getSecondsToRun() > 0 ? Integer.toString(group
              .getSecondsToRun()) + " seconds" : "no") + " time limit, " +
          (group.getRunsPerSecond() >= 0 ? "max " + Integer.toString(group
              .getRunsPerSecond()) : "unlimited") + " runs per second).");
      groupMetrics[j] = new BenchmarkMetric();
      for (int i = 1; i <= group.getNumAgents(); ++i) {
        threadList.add(new BenchmarkThread(group, i, groupMetrics[j]));
      }
    }
    BenchmarkThread[] threads =
        threadList.toArray(new BenchmarkThread[threadList.size()]);

    // 4. start a reporter thread
    ReportThread reporter = new ReportThread(groups, groupMetrics, config);
    reporter.start();

    // 5. start all threads
    for (BenchmarkThread thread : threads) {
      thread.start();
    }

    // 6. wait for all threads to finish
    while (!threadList.isEmpty()) {
      BenchmarkThread thread = threadList.removeFirst();
      try {
        thread.join(10);
      } catch (InterruptedException e) {
        error("InterruptedException caught in Thread.join(). Ignoring.");
      }
      if (thread.isAlive()) {
        threadList.addLast(thread);
      }
    }

    // 7. Stop reporter thread
    reporter.interrupt();
    try {
      reporter.join();
    } catch (InterruptedException e) {
      error("InterruptedException caught in Thread.join(). Ignoring.");
    }

    return true;
  }

  void shutdown() throws BenchmarkException {
    if (benchmark != null)
      benchmark.shutdown();
  }

  public static void main(String[] args) {
    // create a runner
    BenchmarkRunner runner = new BenchmarkRunner();

    try {
      // configure it
      boolean ok = runner.parseOptions(args);

      // run it
      if (ok) ok = runner.run();

    } catch (Exception e) {
      error(e.getMessage());

    } finally {

      // shut it down
      try {
        runner.shutdown();
      } catch (Exception e) {
        error(e.getMessage());
      }
    }
  }
}
