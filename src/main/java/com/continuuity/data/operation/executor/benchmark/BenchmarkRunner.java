package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class BenchmarkRunner {

  String benchName = null;
  String opexName = null;

  OperationExecutor opex;
  OpexProvider opexProvider;
  Benchmark benchmark;


  Thread[] threads = null;
  long[] threadTimes = null;

  void warn(String message) {
    System.err.println("Warning: " + message);
  }
  void error(String message) {
    System.err.println("Error: " + message);
  }

  // --benchmark <class> --opex <name> [ arg ... ]
  String[] parseArgs(String[] args) throws BenchmarkException {
    ArrayList<String> remaining = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if ("--benchmark".equals(args[i])) {
        if (i + 1 < args.length) {
          benchName = args[++i];
        } else throw new
            BenchmarkException("--benchmark must have an argument.");

      } else if ("--opex".equals(args[i])) {
        if (i + 1 < args.length) {
          opexName = args[++i];
        } else throw new
            BenchmarkException("--opex must have an argument.");
      } else
        remaining.add(args[i]);
    }
    if (benchName == null) {
      throw new BenchmarkException("--benchmark must be specified.");
    }
    if (opexName == null) {
      throw new BenchmarkException("--opex must be specified.");
    }
    return remaining.toArray(new String[remaining.size()]);
  }

  String[] createOpex(String[] args) throws BenchmarkException {
    if ("memory".equals(this.opexName)) {
      this.opexProvider = new MemoryOpexProvider();
    } else if ("hbase".equals(this.opexName)) {
      this.opexProvider = new HBaseOpexProvider();
    } else if ("remote".equals(this.opexName)) {
      this.opexProvider = new RemoteOpexProvider();
    } else {
      // consider opexName the class name of an opex provider
      // if it is not a fully qualified class name, add package to it
      if (!this.opexName.startsWith("com.continuuity"))
        opexName = this.getClass().getPackage().getName() + "." + opexName;
      try {
        this.opexProvider =
            (OpexProvider)Class.forName(opexName).newInstance();
      } catch (Exception e) {
        throw new BenchmarkException(
            "Cannot instantiate opex provider '" + opexName + "': " +
            e.getMessage());
      }
    }
    args = this.opexProvider.configure(args);
    this.opex = this.opexProvider.create();
    return args;
  }

  void shutdownOpex() {
    try {
      this.opexProvider.shutdown(this.opex);
    } catch (Exception e) {
      error("Cannot shutdown '" + this.opexName + "' operation executor: " +
          e.getMessage());
    }
  }

  String[] createBenchmark(String[] args) throws BenchmarkException {
    try {
      if (!benchName.startsWith("com.continuuity"))
        benchName = this.getClass().getPackage().getName() + "." + benchName;
      this.benchmark =
          (Benchmark) Class.forName(benchName).newInstance();
    } catch (Exception e) {
      throw new BenchmarkException("Cannot instantiate benchmark '" +
          benchName + "': " + e.getMessage());
    }
    return benchmark.configure(args);
  }

  public boolean run(String[] args) {
    // try to parse arguments
    try {
      args = parseArgs(args);
    } catch (BenchmarkException e) {
      error(e.getMessage());
      return false;
    }

    // try to instantiate opex
    try {
      args = createOpex(args);
    } catch (BenchmarkException e) {
      error(e.getMessage());
      return false;
    }

    // try to instantiate benchmark
    try {
      args = createBenchmark(args);
    } catch (BenchmarkException e) {
      error(e.getMessage());
      return false;
    }

    if (args.length > 0) {
      warn("Unused command line arguments: " + Arrays.toString(args));
    }

    // prepare benchmark
    benchmark.prepare(opex);

    // warm up data fabric
    benchmark.warmup(opex);

    // obtain all agent groups
    AgentGroup[] groups = benchmark.getGroups(opex);

    // create all agents
    LinkedList<BenchThread> threadList = new LinkedList<BenchThread>();
    for (AgentGroup group : groups) {
      for (int i = 0; i < group.getNumInstances(); i++) {
        Runnable agent = group.getAgent(opex, i);
        BenchThread thread = new BenchThread(agent, threadList.size());
        threadList.add(thread);
      }
    }
    this.threads = threadList.toArray(new Thread[threadList.size()]);
    this.threadTimes = new long[threadList.size()];

    // start all agents
    for (Thread thread : this.threads) thread.start();

    // wait for all agents
    int remaining = this.threads.length;
    while (remaining > 0) {
      BenchThread thread = threadList.removeFirst();
      try {
        thread.join(10);
      } catch (InterruptedException e) {
        error("InterruptedException caught in Thread.join(). Ignoring.");
      }
      if (thread.isAlive()) {
        threadList.addLast(thread);
      } else {
        --remaining;
      }
    }

    // shutdown benchmark
    shutdownOpex();

    // report custom metrics
    benchmark.report(opex);

    // report runtime metrics
    int i = 0;
    for (AgentGroup group : groups) {
      StringBuilder builder = new StringBuilder(
          "Runtimes for group '" + group.getName() + "':");
      long groupTotal = 0;
      for (int j = 0; j < group.getNumInstances(); j++) {
        long time = this.threadTimes[i++];
        groupTotal += time;
        builder.append(' ');
        builder.append(time);
      }
      builder.append(", Total group time = ");
      builder.append(groupTotal);
      builder.append(", Average ");
      builder.append(groupTotal / group.getNumInstances());
      builder.append('.');
      System.out.println(builder.toString());
    }

    return true;
  }

  class BenchThread extends Thread {
    Runnable agent;
    int index;
    BenchThread(Runnable agent, int index) {
      this.agent = agent;
      this.index = index;
    }
    public void run() {
      long start = System.currentTimeMillis();
      agent.run();
      long end = System.currentTimeMillis();
      long time = end - start;
      BenchmarkRunner.this.threadTimes[index] = time;
    }
  }

  public static void main(String[] args) {
    if (new BenchmarkRunner().run(args)) {
      System.out.println("Success.");
    } else {
      System.err.println("Failed.");
    }
  }

}
