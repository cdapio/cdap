package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class OpexServiceMain {

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = OpexServiceMain.class.getSimpleName();
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }


  public static void main(String args[]) {

    if (args.length != 1) {
      usage(true);
      return;
    }
    if ("--help".equals(args[0])) {
      usage(false);
      return;
    }

    final int NOOP = 0;
    final int START = 1;
    final int STOP = 2;

    int command = NOOP;

    if ("start".equals(args[0])) {
      command = START;
    } else if ("stop".equals(args[0])) {
      command = STOP;
    } else {
      usage(true);
      return;
    }

    DataFabricDistributedModule module = new DataFabricDistributedModule();
    Injector injector = Guice.createInjector(module);

    // start an opex service
    final OperationExecutorService opexService =
        injector.getInstance(OperationExecutorService.class);

    if (START == command) {

      // enable metrics collection
      CConfiguration configuration = module.getConfiguration();
      OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, configuration);

      Copyright.print(System.out);
      System.out.println("Starting Operation Executor Service...");
      // start it. start is blocking, hence main won't terminate
      try {
        opexService.start(new String[] { }, configuration);
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
        return;
      }
    }
    else if (STOP == command) {
      Copyright.print(System.out);
      System.out.println("Stopping Operation Executor Service...");
      opexService.stop(true);
    }
  }
}
