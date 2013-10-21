package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.service.CommandPortService;
import com.continuuity.common.service.RUOKHandler;
import com.continuuity.common.utils.Copyright;
import com.continuuity.data.runtime.DataFabricOpexModule;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.util.hbase.ConfigurationTable;
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
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop tx in distributed mode.
 */
public class OpexServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpexServiceMain.class);

  private static final int START = 1;
  private static final int STOP = 2;

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = OpexServiceMain.class.getSimpleName();
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

    int command;

    if ("start".equals(args[0])) {
      command = START;
    } else if ("stop".equals(args[0])) {
      command = STOP;
    } else {
      usage(true);
      return;
    }

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    DataFabricOpexModule module = new DataFabricOpexModule(cConf, hConf);

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
              .setSessionTimeout(cConf.getInt(
                Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    KafkaClientService kafkaClientService = new ZKKafkaClientService(
      kafkaZKNamespace == null
        ? zkClientService
        : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
    );

    Injector injector = Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new IOModule(),
      new ConfigModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      module);

    // start a tx server
    final TransactionService txService = injector.getInstance(TransactionService.class);

    if (START == command) {
      ShutdownHookManager.get().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            if (txService.isRunning()) {
              txService.stopAndWait();
            }
          } catch (Throwable e) {
            LOG.error("Failed to shutdown transaction service.", e);
            // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
            System.err.println("Failed to shutdown transaction service: " + e.getMessage());
            e.printStackTrace(System.err);
          }
        }
      }, FileSystem.SHUTDOWN_HOOK_PRIORITY + 1);

      // starting health/status check service
      CommandPortService service = startHealthCheckService(cConf);

      // Starts metrics collection
      MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService));

      Copyright.print(System.out);
      System.out.println("Starting Operation Executor Service...");

      // populate the current configuration into an HBase table, for use by HBase components
      ConfigurationTable configTable = new ConfigurationTable(injector.getInstance(
          Key.get(Configuration.class, Names.named("HBaseOVCTableHandleHConfig"))));
      configTable.write(ConfigurationTable.Type.DEFAULT, cConf);

      // start it. start is not blocking, hence we want to block to avoid termination of main
      Future<?> future = Services.getCompletionFuture(txService);
      try {
        txService.start();
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
      }

      future.get();

      service.stop();

    } else {
      Copyright.print(System.out);
      System.out.println("Stopping Operation Executor Service...");
      txService.stop();
    }
  }

  private static CommandPortService startHealthCheckService(CConfiguration conf) {
    int port = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_COMMAND_PORT, 0);
    CommandPortService service = CommandPortService.builder("tx-status")
      .setPort(port)
      .addCommandHandler(RUOKHandler.COMMAND, RUOKHandler.DESCRIPTION, new RUOKHandler())
      .build();
    service.startAndWait();
    return service;
  }
}
