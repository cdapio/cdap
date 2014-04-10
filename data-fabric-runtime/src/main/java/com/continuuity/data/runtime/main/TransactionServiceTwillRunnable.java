package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataFabricOpexModule;
import com.continuuity.data2.transaction.distributed.TransactionService;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/**
 * TwillRunnable to run Transaction Service through twill.
 */
public class TransactionServiceTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceTwillRunnable.class);

  private String name;
  private String cConfName;
  private String hConfName;
  private CountDownLatch runLatch;

  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private TransactionService txService;

  public TransactionServiceTwillRunnable(String name, String cConfName, String hConfName) {
    this.name = name;
    this.cConfName = cConfName;
    this.hConfName = hConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of("cConf", cConfName, "hConf", hConfName))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    runLatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initializing runnable " + name);
    try {
      // Load configuration
      Configuration hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      CConfiguration cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      LOG.info("Setting host name to " + context.getHost().getCanonicalHostName());
      cConf.set(Constants.Transaction.Twill.ADDRESS, context.getHost().getCanonicalHostName());

      // Set Gateway port to 0, so that it binds to any free port.
      cConf.setInt(Constants.Transaction.Twill.PORT, 0);

      LOG.info("Continuuity conf {}", cConf);
      LOG.info("HBase conf {}", hConf);

      Injector injector = createGuiceInjector(cConf, hConf);

      // Initialize ZK client and Kafka client
      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);

      // Get the metrics collection service
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

      // Get the Transaction Service
      txService = injector.getInstance(TransactionService.class);

      LOG.info("Runnable initialized " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable " + name);
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
          LOG.error("Failed to shutdown transaction service: " + e.getMessage());
          e.printStackTrace(System.err);
        }
      }
    }, FileSystem.SHUTDOWN_HOOK_PRIORITY + 1);

    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService));
    LOG.info("Runnable started " + name);

    LOG.info("Starting Operation Executor Service...");

    // start it. start is not blocking, hence we want to block to avoid termination of main
    Future<?> future = Services.getCompletionFuture(txService);
    try {
      txService.start();
      future.get();
    } catch (Exception e) {
      LOG.error("Failed to start service: " + e.getMessage());
    }

    try {
      runLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Waiting on latch interrupted");
      Thread.currentThread().interrupt();
    }

    LOG.info("Runnable stopped " + name);
  }

  @Override
  public void stop() {
    LOG.info("Stopping runnable " + name);
    LOG.info("Stopping Operation Executor Service...");
    txService.stop();
    Futures.getUnchecked(Services.chainStop(metricsCollectionService,
                                            kafkaClientService, zkClientService));
    runLatch.countDown();
  }

  static Injector createGuiceInjector(CConfiguration cConf, Configuration hConf) {
    DataFabricOpexModule module = new DataFabricOpexModule(cConf, hConf);
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      module
    );
  }
}
