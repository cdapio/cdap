package com.continuuity.data2.transaction;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.service.CommandPortService;
import com.continuuity.common.service.RUOKHandler;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.data2.transaction.runtime.TransactionDistributedModule;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.twill.common.Services;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop tx in distributed mode.
 */
public class TransactionServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceMain.class);

  private static final int START = 1;
  private static final int STOP = 2;

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = TransactionServiceMain.class.getSimpleName();
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }


  public static void main(String args[]) throws Exception {

    // TODO: Put this back in
//    if (args.length != 1) {
//      usage(true);
//      return;
//    }
//    if ("--help".equals(args[0])) {
//      usage(false);
//      return;
//    }
//
//    int command;
//
//    if ("start".equals(args[0])) {
//      command = START;
//    } else if ("stop".equals(args[0])) {
//      command = STOP;
//    } else {
//      usage(true);
//      return;
//    }

    int command = START;

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
              .setSessionTimeout(cConf.getInt(
                Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
            org.apache.twill.zookeeper.RetryStrategies.fixDelay(2, TimeUnit.MILLISECONDS)
          )
        )
      );
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    KafkaClientService kafkaClientService = new ZKKafkaClientService(
        kafkaZKNamespace == null
          ? zkClientService
          : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
      );

    // TODO: Tx local module?
    Injector injector = Guice.createInjector(
      new ZKClientModule(),
      new IOModule(),
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new TransactionDistributedModule());

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

      // TODO: Put this back in?
      Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService));

      System.out.println("Starting Operation Executor Service...");

      System.err.println(zkClientService.isRunning() + ", " + kafkaClientService.isRunning());

      // populate the current configuration into an HBase table, for use by HBase components
      ConfigurationTable configTable = new ConfigurationTable(hConf);
      // TODO: Put this back in?
//      ConfigurationTable configTable = new ConfigurationTable(injector.getInstance(
//        Key.get(Configuration.class, Names.named("HBaseOVCTableHandleHConfig"))));
      configTable.write(ConfigurationTable.Type.DEFAULT, cConf);
      System.err.println("Got after config table write");
      // start it. start is not blocking, hence we want to block to avoid termination of main
      Future<?> future = Services.getCompletionFuture(txService);
      try {
        System.err.println("Got before tx service start");
        Thread.sleep(10000);
        txService.start();
        Thread.sleep(10000);
        System.err.println("Got after tx service start");
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
      }
      System.err.println("Got before future get");
      future.get();
      System.err.println("Got after future get");
      service.stop();
      System.err.println("Got after service stop");
    } else {
      System.out.println("Stopping Operation Executor Service...");
      txService.stop();
    }
  }

  private static CommandPortService startHealthCheckService(CConfiguration conf) {
    // TODO: Check port
    int port = conf.getInt("data.tx.command.port", 0);
    CommandPortService service = CommandPortService.builder("tx-status")
      .setPort(port)
      .addCommandHandler(RUOKHandler.COMMAND, RUOKHandler.DESCRIPTION, new RUOKHandler())
      .build();
    service.startAndWait();
    return service;
  }
}
