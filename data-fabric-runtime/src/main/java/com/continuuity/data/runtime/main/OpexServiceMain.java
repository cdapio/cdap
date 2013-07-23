package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.common.utils.Copyright;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop opex in distributed mode.
 */
public class OpexServiceMain {

  private static final int NOOP = 0;
  private static final int START = 1;
  private static final int STOP = 2;

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
    CConfiguration configuration = module.getConfiguration();

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE))
                                   .setSessionTimeout(10000)
                                   .build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );
    String kafkaZKNamespace = configuration.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    KafkaClientService kafkaClientService = new ZKKafkaClientService(
      kafkaZKNamespace == null
        ? zkClientService
        : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
    );

    Injector injector = Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new IOModule(),
      new ConfigModule(),
      module);

    // start an opex service
    final OperationExecutorService opexService =
      injector.getInstance(OperationExecutorService.class);

    if (START == command) {
      // Starts metrics collection
      MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService));

      // enable metrics collection
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
    } else if (STOP == command) {
      Copyright.print(System.out);
      System.out.println("Stopping Operation Executor Service...");
      opexService.stop(true);
    }
  }
}
