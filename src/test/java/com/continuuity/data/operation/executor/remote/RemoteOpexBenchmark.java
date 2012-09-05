package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.ReadKey;
import com.continuuity.api.data.Write;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

public abstract class RemoteOpexBenchmark {

  OperationExecutor local, remote;
  InMemoryZookeeper zookeeper;
  CConfiguration config;
  OperationExecutorService opexService;

  public void startService(CConfiguration conf, OperationExecutor opex)
      throws Exception {

    // start an in-memory zookeeper and remember it in a config object
    zookeeper = new InMemoryZookeeper();
    config = conf;
    config.set(Constants.CFG_ZOOKEEPER_ENSEMBLE,
        zookeeper.getConnectionString());

    // find a free port to use for the service
    int port = PortDetector.findFreePort();
    config.setInt(Constants.CFG_DATA_OPEX_SERVER_PORT, port);

    // start an opex service
    opexService = new OperationExecutorService(opex);

    // and start it. Since start is blocking, we have to start async'ly
    new Thread () {
      public void run() {
        try {
          opexService.start(new String[] { }, config);
        } catch (Exception e) {
          System.err.println("Failed to start service: " + e.getMessage());
        }
      }
    }.start();

    // and wait until it has fully initialized
    StopWatch watch = new StopWatch();
    watch.start();
    while(watch.getTime() < 10000) {
      if (opexService.ruok()) break;
    }
    Assert.assertTrue("Operation Executor Service failed to come up within " +
        "10 seconds.", opexService.ruok());

    // now create a remote opex that connects to the service
    remote = new RemoteOperationExecutor(config);
    local = opex;
  }

  public void stopService() throws Exception {

    // shutdown the opex service
    if (opexService != null)
      opexService.stop(true);

    // and shutdown the zookeeper
    if (zookeeper != null) {
      zookeeper.close();
    }
  }

  abstract public void startService() throws Exception;


  static long performNIncrements(OperationExecutor opex,
                                 int threadId, int numWrites)
      throws OperationException {

    System.err.println("Thread"  + threadId +
        ": Performing " + numWrites + " increments with opex: " + opex);
    final byte[] key = ("key" + threadId).getBytes();
    final byte[] value = { 0, 0, 0, 0, 0, 0, 0, 0 };
    Write write = new Write(key, value);
    opex.execute(write);
    Increment increment = new Increment(key, 1);
    long before = System.currentTimeMillis();
    for (int i = 0; i < numWrites; i++) {
      opex.execute(increment);
    }
    long after = System.currentTimeMillis();
    ReadKey read = new ReadKey(key);
    byte[] bytes = opex.execute(read).getValue();
    Assert.assertNotNull(bytes);
    Assert.assertEquals(8, bytes.length);
    Assert.assertEquals(numWrites, Bytes.toLong(bytes));
    return after - before;
  }

  static long performNWrites(OperationExecutor opex,
                             int threadId, int numWrites)
      throws OperationException {

    System.err.println("Thread"  + threadId +
        ": Performing " + numWrites + " writes with opex: " + opex);
    final byte[] key = ("key" + threadId).getBytes();
    final byte[] value = { 0 };
    Write write = new Write(key, value);
    long before = System.currentTimeMillis();
    for (int i = 0; i < numWrites; i++) {
      value[0] = (byte)(i%0x000000ff);
      opex.execute(write);
    }
    long after = System.currentTimeMillis();
    return after - before;
  }

  enum Test {
    WRITE, INCREMENT
  }

  long performOperations(OperationExecutor opex,
                         Test test, int threadId, int numOps)
      throws OperationException {
    switch(test) {
      case WRITE: return performNWrites(opex, threadId, numOps);
      case INCREMENT: return performNIncrements(opex, threadId, numOps);
      default: return 0L;
    }
  }

  class BenchmarkThread extends Thread {
    OperationExecutor opex;
    Test test;
    int numOps;
    int threadId;
    long[] runtimes;
    BenchmarkThread(OperationExecutor opex, Test test, int numOps, int threadId, long[] runtimes) {
      this.opex = opex;
      this.test = test;
      this.numOps = numOps;
      this.threadId = threadId;
      this.runtimes = runtimes;
    }
    public void run() {
      try {
        long milliSeconds = performOperations(opex, test, threadId, numOps);
        runtimes[threadId] = milliSeconds;
      } catch (OperationException e) {
        Assert.fail("Thread got exception: " + e.getMessage());
      }
    }
  }

  long benchmark(OperationExecutor opex, Test test, int numThreads, int numOps) {
    BenchmarkThread[] threads = new BenchmarkThread[numThreads];
    long[] runtimes = new long[numThreads];
    for (int i = 0; i < numThreads; i++)
      threads[i] = new BenchmarkThread(opex, test, numOps, i, runtimes);
    for (int i = 0; i < numThreads; i++)
      threads[i].start();
    for (int i = 0; i < numThreads; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        System.err.println("thread[" + i + "].join() interrupted. ");
      }
    }
    long total = 0;
    for (int i = 0; i < numThreads; i++) {
      total += runtimes[i];
    }
    return total;
  }

  void benchmark(Test test, int numThreads, int numOps) {
    System.out.println("Warming up with 1 thread ...");
    benchmark(local, test, 1, numOps);
    System.out.println("Benchmarking local opex with " + numThreads + " threads ...");
    long millisLocal = benchmark(local, test, numThreads, numOps);
    System.out.println("Benchmarking remote opex with " + numThreads + " threads ...");
    long millisRemote = benchmark(remote, test, numThreads, numOps);
    System.err.println("Millis with local opex:  " + millisLocal);
    System.err.println("Millis with remote opex: " + millisRemote);
  }

  public static void main(String[] args) {
    RemoteOpexBenchmark benchmark;
    if ("--memory".equals(args[0])) {
      benchmark = new MemoryBenchmark();
    } else if ("--hbase".equals(args[0])) {
      benchmark = new HBaseBenchmark();
    } else {
      System.err.println("Either --memory or --hbase must be specified.");
      return;
    }
    Test test;
    if ("--write".equals(args[1])) {
      test = Test.WRITE;
    } else if ("--increment".equals(args[1])) {
      test = Test.INCREMENT;
    } else {
      System.err.println("Test type must be specified. For instance try " +
          "--write or --increment");
      return;
    }
    int numThreads = Integer.valueOf(args[2]);
    int numOps = Integer.valueOf(args[3]);

    try {
      benchmark.startService();
      benchmark.benchmark(test, numThreads, numOps);
      benchmark.stopService();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace(System.err);
    }
  }
}

class MemoryBenchmark extends RemoteOpexBenchmark {

  public void startService() throws Exception {
    Module module = new DataFabricModules().getInMemoryModules();
    Injector injector = Guice.createInjector(module);
    startService(CConfiguration.create(),
        injector.getInstance(OperationExecutor.class));
  }
}

class HBaseBenchmark extends RemoteOpexBenchmark {

  public void startService() throws Exception {
    HBaseTestBase.startHBase();
    DataFabricDistributedModule module =
        new DataFabricDistributedModule(HBaseTestBase.getConfiguration());
    Injector injector = Guice.createInjector(module);
    startService(module.getConfiguration(),
      injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor"))));
  }

  @Override
  public void stopService() throws Exception {
    super.stopService();
    HBaseTestBase.stopHBase();
  }
}
