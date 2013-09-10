package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;

/**
 * Driver to start and stop NOOP opex. The opex that is started using this driver returns only empty results.
 * It is useful for testing and performance benchmarks.
 */
public class NoOpexServiceMain {

  private static final int NOOP = 0;
  private static final int START = 1;
  private static final int STOP = 2;

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = NoOpexServiceMain.class.getSimpleName();
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }

  public static void main(String args[]) throws Exception {

    if (args.length != 1) {
      usage(true);
      return;
    }
    if ("--help".equals(args[0])) {
      usage(false);
      return;
    }

    int command = NOOP;

    if ("start".equals(args[0])) {
      command = START;
    } else if ("stop".equals(args[0])) {
      command = STOP;
    } else {
      usage(true);
      return;
    }

    OperationExecutorService opexService =
      new OperationExecutorService(new NoOperationExecutor());

    if (START == command) {

      Copyright.print(System.out);

      // Start ZooKeeper
      System.out.print("Starting ZooKeeper...");
      System.out.flush();
      InMemoryZookeeper zkCluster = new InMemoryZookeeper();
      System.out.println("running at " + zkCluster
        .getConnectionString() + "");

      // Add ZK info to conf
      CConfiguration configuration = CConfiguration.create();
      configuration.set(Constants.Zookeeper.QUORUM,
                        zkCluster.getConnectionString());

      System.out.println("Starting Operation Executor Service...");
      // start it. start is blocking, hence main won't terminate
      try {
        opexService.start(new String[] { }, configuration);
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
        return;
      }
    } else if (STOP == command) {
      Copyright.print(System.out);
      System.out.println("Stopping Operation Executor Service...");
      opexService.stop(true);
    }
  }

  private static File getRandomTempDir() {
    File file = new File(System.getProperty("java.io.tmpdir"),
                         Integer.toString(Math.abs(
                           new Random(System.currentTimeMillis()).nextInt())));
    if (!file.mkdir()) {
      throw new RuntimeException("Unable to create temp directory");
    }
    return file;
  }

}
