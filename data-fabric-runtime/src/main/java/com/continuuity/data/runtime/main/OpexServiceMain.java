package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseTableUtil;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start and stop opex in distributed mode.
 */
public class OpexServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpexServiceMain.class);

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

    DataFabricDistributedModule module = new DataFabricDistributedModule();
    CConfiguration configuration = module.getConfiguration();

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(configuration.get(Constants.Zookeeper.QUORUM))
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
      new LocationRuntimeModule().getDistributedModules(),
      module);

    // start an opex service
    final OperationExecutorService opexService =
      injector.getInstance(OperationExecutorService.class);

    if (START == command) {
      final InMemoryTransactionManager txManager = injector.getInstance(InMemoryTransactionManager.class);
      txManager.init();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            txManager.close();
          } catch (Throwable e) {
            LOG.error("Failed to shutdown transaction manager.", e);
            // because shutdown hooks execute concurrently, the logger may be closed already: thus also print it.
            System.err.println("Failed to shutdown transaction manager: " + e.getMessage()
                                 + ". At " + StackTraceUtil.toStringStackTrace(e));
          }
        }
      });

      // Starts metrics collection
      MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService));

      Copyright.print(System.out);
      System.out.println("Starting Operation Executor Service...");

      // Creates HBase queue table
      QueueAdmin queueAdmin = injector.getInstance(QueueAdmin.class);
      String queueTableName = HBaseTableUtil.getHBaseTableName(
        configuration, configuration.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME)
      );

      if (!queueAdmin.exists(queueTableName)) {
        queueAdmin.create(queueTableName);
      }

      // start it. start is blocking, hence main won't terminate
      try {
        opexService.start(new String[] { }, configuration);
      } catch (Exception e) {
        System.err.println("Failed to start service: " + e.getMessage());
      }
    } else {
      Copyright.print(System.out);
      System.out.println("Stopping Operation Executor Service...");
      opexService.stop(true);
    }
  }
}
