package com.continuuity.gateway.v2.run;

import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.v2.Gateway;
import com.continuuity.gateway.v2.runtime.GatewayModules;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WeaveRunnable to run Gateway through weave.
 */
public class GatewayWeaveRunnable extends AbstractWeaveRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayWeaveRunnable.class);

  private String name;
  private String cConfName;
  private String hConfName;
  private CountDownLatch runLatch;

  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private Gateway gateway;

  public GatewayWeaveRunnable(String name, String cConfName, String hConfName) {
    this.name = name;
    this.cConfName = cConfName;
    this.hConfName = hConfName;
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of("cConf", cConfName, "hConf", hConfName))
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
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
      cConf.set(Constants.Gateway.ADDRESS, context.getHost().getCanonicalHostName());

      // Set Gateway port to 0, so that it binds to any free port.
      cConf.setInt(Constants.Gateway.PORT, 0);

      LOG.info("Continuuity conf {}", cConf);
      LOG.info("HBase conf {}", hConf);

      // Initialize ZK client
      String zookeeper = cConf.get(Constants.Zookeeper.QUORUM);
      if (zookeeper == null) {
        LOG.error("No zookeeper quorum provided.");
        throw new IllegalStateException("No zookeeper quorum provided.");
      }

      zkClientService =
        ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(zookeeper).build(),
              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
          ));

      // Initialize Kafka client
      String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
      kafkaClientService = new ZKKafkaClientService(
        kafkaZKNamespace == null
          ? zkClientService
          : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
      );

      Injector injector = createGuiceInjector(kafkaClientService, zkClientService, cConf, hConf);
      // Get the metrics collection service
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

      // Get the Gatewaty
      gateway = injector.getInstance(Gateway.class);

      LOG.info("Runnable initialized " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable " + name);
    Futures.getUnchecked(Services.chainStart(zkClientService, kafkaClientService,
                                             metricsCollectionService, gateway));
    LOG.info("Runnable started " + name);

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

    Futures.getUnchecked(Services.chainStop(gateway, metricsCollectionService,
                                            kafkaClientService, zkClientService));
    runLatch.countDown();
  }

  static Injector createGuiceInjector(KafkaClientService kafkaClientService, ZKClientService zkClientService,
                                      CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
      new GatewayModules().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
          // AppFabricServiceModule
          bind(StoreFactory.class).to(MDTBasedStoreFactory.class);
        }
      }
    );
  }
}
