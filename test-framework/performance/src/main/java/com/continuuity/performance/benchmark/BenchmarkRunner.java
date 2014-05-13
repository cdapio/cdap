package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class that manages the execution of a benchmark.
 */
public class BenchmarkRunner {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

  private String benchName = null;
  private Benchmark benchmark = null;
  private CConfiguration config = CConfiguration.create();

  void usage() {
    System.out.println("Usage: BenchmarkRunner --bench <name> [ --report " + "<seconds> ] [ --<key> <value> ... ]");
    if (benchmark != null) {
      Map<String, String> usage = benchmark.usage();
      if (usage != null && !usage.isEmpty()) {
        LOG.info("Specific options for benchmark " + benchName + ":");
        for (String option : usage.keySet()) {
          LOG.info(String.format("  %-20s %s", option, usage.get(option)));
        }
      }
    } else {
      System.out.println("Use --help --bench <name> for benchmark specific " + "options.");
    }
  }

  private boolean parseOptions(String[] args) throws BenchmarkException {
    boolean help = false;

    // 1. parse command line for --bench, copy everything else into config
    LOG.debug("Parsing command line options...");
    for (int i = 0; i < args.length; i++) {
      if ("--help".equals(args[i])) {
        help = true;
        continue;
      } else if (args[i].startsWith("--")) {
        if (i + 1 < args.length) {
          String key = args[i].substring(2);
          String value = args[++i];
          config.set(key, value);
          if ("bench".equals(key)) {
            benchName = value;
          }
        } else {
          throw new BenchmarkException("--<key> must have an argument.");
        }
      }
    }

    LOG.debug("Instantiating and configuring benchmark...");
    // 2. instantiate benchmark and configure it
    if (benchName == null) {
      if (help) {
        usage();
        return false;
      } else {
        throw new BenchmarkException("--bench must be specified.");
      }
    }
    if (!benchName.startsWith("com.continuuity")) {
      benchName = this.getClass().getPackage().getName() + "." + benchName;
    }
    try {
      benchmark = (Benchmark) Class.forName(benchName).newInstance();
    } catch (Exception e) {
      throw new BenchmarkException("Unable to instantiate benchmark '" +
          benchName + "': " + e.getMessage(), e);
    }
    if (help) {
      usage();
      benchmark = null;
      return false;
    }
    benchmark.configure(config);
    return true;
  }

  private int countAgents(AgentGroup[] groups) {
    int count = 0;
    for (AgentGroup group : groups) {
      count += group.getNumAgents();
    }
    return count;
  }

  @SuppressWarnings("unchecked")
  private BenchmarkResult run(BenchmarkResult result) throws Exception {
    // 1. initialize benchmark
    LOG.debug("Executing benchmark.initialize()");
    benchmark.initialize();

    // 2. warm up benchmark
    LOG.debug("Executing benchmark.warmup()");
    benchmark.warmup();

    // 3. get agent groups and create a thread for each agent
    AgentGroup[] groups = benchmark.getAgentGroups();
    final int totalNumAgents = countAgents(groups);

    BenchmarkMetric[] groupMetrics = new BenchmarkMetric[groups.length];
    List<Future> agentFutureList = new ArrayList<Future>(totalNumAgents);

    ExecutorService agentThreadPool = Executors.newFixedThreadPool(
      totalNumAgents, new ThreadFactoryBuilder().setNameFormat("benchmark-runner-%d").build());
    CompletionService agentCompletionPool = new ExecutorCompletionService(agentThreadPool);

    for (int j = 0; j < groups.length; j++) {
      AgentGroup group = groups[j];
      int numAgents = group.getNumAgents();
      if (numAgents < 1) {
        throw new BenchmarkException("Number of agents for group " + group.getName()
                                       + " must be at leat one but is " + numAgents + ".");
      }
      int runsPerAgent = group.getTotalRuns() / numAgents;
      LOG.info("Running " + numAgents + " " + group.getName() + " agents (" +
                 (runsPerAgent > 0 ? Integer.toString(runsPerAgent) : "unlimited") + " runs per agent, " +
                 (group.getSecondsToRun() > 0 ? Integer.toString(group.getSecondsToRun()) + " seconds" : "no") + " " +
                 "time limit, " +
                 (group.getRunsPerSecond() > 0 ? "max " + Integer.toString(group.getRunsPerSecond()) : "unlimited") +
                 " runs per second).");

      groupMetrics[j] = new BenchmarkMetric();

      for (int i = 0; i < group.getNumAgents(); ++i) {
        BenchmarkRunnable br = new BenchmarkRunnable(group, i, groupMetrics[j], false);
        LOG.debug("Starting thread for benchmark agent {} of group {}", i, j);
        agentFutureList.add(agentCompletionPool.submit(br, null));
      }
    }

    List<MetricsCollector> collectorList = new ArrayList<MetricsCollector>(3);
    List<Future> collectorFutureList = new ArrayList<Future>(3);
    ExecutorService collectorThreadPool = Executors.newFixedThreadPool(3);
    CompletionService collectorCompletionPool = new ExecutorCompletionService(collectorThreadPool);

    // 4. start the console and other reporter threads
    LOG.debug("Starting console reporter thread");
    MetricsCollector consoleReporter = new ConsoleMetricReporter(groups, groupMetrics, config);
    collectorList.add(consoleReporter);
    collectorFutureList.add(collectorCompletionPool.submit(consoleReporter, null));

    if (StringUtils.isNotEmpty(config.get("reportfile"))) {
      LOG.debug("Starting file reporter thread");
      MetricsCollector fileReporter = new FileMetricReporter(benchName, groups, groupMetrics, config);
      collectorList.add(fileReporter);
      collectorFutureList.add(collectorCompletionPool.submit(fileReporter, null));
    }

    // 5. wait for first benchmark thread to finish
    LOG.debug("Waiting for first benchmark thread to finish...");
    agentCompletionPool.take();

    // 6. stop all reporter threads
    LOG.debug("Stopping all collector threads...");
    for (int i = 0; i < collectorList.size(); i++) {
      collectorList.get(i).stop();
      collectorFutureList.get(i).cancel(true);
    }

    consoleReporter.getResults(result);

    // 7. wait for remaining benchmark threads to finish
    LOG.debug("Waiting for remaining benchmark threads to finish...");
    for (int i = 1; i < totalNumAgents; i++) {
      agentCompletionPool.take();
    }

    LOG.debug("All benchmark threads stopped.");

    collectorThreadPool.shutdown();
    agentThreadPool.shutdown();

    return result;
  }

  void shutdown() {
    if (benchmark != null) {
      LOG.debug("Executing shutdown method of benchmark.");
      benchmark.shutdown();
    }
  }

  public BenchmarkResult doRun(String[] args) throws Exception {
    try {
      // configure it
      if (parseOptions(args)) {
        return run(new BenchmarkResult(benchName, args));
      }
      return null;
    } finally {
      // shut it down
      shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    new BenchmarkRunner().doRun(args);
    LOG.info("Benchmark executed successfully.");
  }
}
